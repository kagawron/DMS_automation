import collections
import json
import logging
import os
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler

import boto3

import config 
from settings import task_settings

# Setup the AWS CLI session using stored credentials
session = boto3.Session(profile_name=config.profile,region_name=config.region)
client = session.client('dms')
sns = session.client('sns')

# ----------------------------------------------------------------------------------------------------------------------#
# Initial setup
# ----------------------------------------------------------------------------------------------------------------------#
try:
    # Create logs, json_files if don't exist
    os.chdir(config.csv_tables_location)
    if not os.path.exists('logs'):
        os.mkdir('logs')

    if not os.path.exists('json_files'):
        os.mkdir('json_files')

except Exception as error:
    print('Something went wrong while creating directories logs, json_files. Check permissions or try to manually create the folders "logs" and "json_files')
    sys.exit(1)



# Create file to store task ARNs
task_name = sys.argv[2]
task_arn_file = 'task_arn_' + task_name + '.txt'

# ----------------------------------------------------------------------------------------------------------------------#
# Setup logging
# ----------------------------------------------------------------------------------------------------------------------#
logfile_location = 'logs/output.log'
log_level = logging.DEBUG
log_format = '%(asctime)s %(levelname)s: [in %(filename)s:%(lineno)d] : %(message)s'

logger = logging.getLogger()
handler = RotatingFileHandler(logfile_location, maxBytes=100000, backupCount=10)
formatter = logging.Formatter(log_format)
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(log_level)

# ----------------------------------------------------------------------------------------------------------------------#
# Create named tuples to hold Table, and filter attributes
# ----------------------------------------------------------------------------------------------------------------------#

# Each table should be associated with a schema. Table will have filters applied to it.
Table = collections.namedtuple('Table', 'schema, table, filters, auto_partitioned')

# Each filter is composed of three attributes
#  1. Column name
#  2. Operator name (eq, ste, gte, between)
#  3. Filter value (Note - In case of between, two values are needed. They should be separated with "~")
Filter = collections.namedtuple('Filter', 'column, operator, value')

# holds tables that have filter conditions
filter_tables = []

# This is a map with key as schema name. This map holds all the tables under a schema.
non_filter_tables = {}


def delete_previous_files():
     # Deletes json and task_arn files in working directory from a previous run.
    print ('Clearing up old files:')


    for file in os.listdir('./json_files'):
        if file.startswith(task_name):
            os.remove(os.path.join('.', 'json_files', file))
            print('File {} deleted'.format(file))


def add_to_non_filter_tables(schema, obj):
    # Adds a table object to the dictionary.
    # Create an entry for the schema.
    if schema not in non_filter_tables.keys():
        non_filter_tables[schema] = []

    # Append the table object.
    non_filter_tables[schema].append(obj)
    

def process_csv_file(csv_file, action):
    """
    Reads Input "csv" file(s) and segregates tables into (a) tables that have no filter conditions
    (b) tables that have filter conditions.

    It is assumed that tables with filter conditions are huge. as a result, they should have a dedicated DMS
    task created for them. On the other hand, all tables with no filter conditions under a schema should be handled by
    a single DMS task.
    """

    with open(csv_file, 'r') as in_file:
        for line in in_file:
            # Following cases fall into this category.
            #  1. Table with no filter conditions
            #  2. All tables in a schema (E.g. HR,%)

            # Handle the tables with no filter conditions
            if len(line.split(',')) == 2:
                schema, table = line.split(',')

                # Remove any special chars and store the schema and table details
                schema = schema.strip()
                table = table.strip('\n').strip()
                table_obj = Table(schema=schema, table=table, filters=[], auto_partitioned=False)
               
                add_to_non_filter_tables(schema, table_obj)

            # Handle the tables with filter conditions
            if len(line.split(',')) > 3:
                cols = line.split(',')
                schema, table = cols[0], cols[1]          # First two positions have schema, table respectively.

                schema = schema.strip()
                table = table.strip()

                # Identify filter count. Each filter is a set of 3 columns.
                filter_count = int(len(cols) - 2) / 3
                index = 2

                # holds all filter conditions of a given table.
                filters = list()

                for i in range(0, int(filter_count)):
                    column, operator, value = cols[index], cols[index + 1], cols[index + 2]

                    column = column.strip()
                    operator = operator.strip()
                    value = value.strip('\n').strip()

                    # Store the filter details in a named table.
                    filter_condition = Filter(column=column, operator=operator, value=value)
                    filters.append(filter_condition)

                    # Add the index to process next filter condition.
                    index += 3

                # Create a Table object.
                table_obj = Table(schema=schema, table=table, filters=filters, auto_partitioned=False)
                filter_tables.append(table_obj)

            # If an entry has exactly 3 columns, at this point, it is assumed that the 3rd column
            # specifies "partition-auto" specified. 
            if len(line.split(',')) == 3:
                schema, table, auto_partition_flag = line.split(',')
                table_obj = Table(schema=schema, table=table, filters=[], auto_partitioned=True)
                add_to_non_filter_tables(schema, table_obj)


def convert_schemas_to_lowercase():
    return {
        "rule-type": "transformation",
        "rule-id": "3",
        "rule-name": "convert-schemas-to-lower",
        "rule-action": "convert-lowercase",
        "rule-target": "schema",
        "object-locator": {
            "schema-name": "%"
        }
    }


def convert_tables_to_lowercase():
    return {
        "rule-type": "transformation",
        "rule-id": "4",
        "rule-name": "convert-tables-to-lower",
        "rule-action": "convert-lowercase",
        "rule-target": "table",
        "object-locator": {
            "schema-name": "%",
            "table-name": "%"
        }
    }


def convert_columns_to_lowercase():
    return {
        "rule-type": "transformation",
        "rule-id": "5",
        "rule-name": "convert-columns-to-lowercase",
        "rule-action": "convert-lowercase",
        "rule-target": "column",
        "object-locator": {
            "schema-name": "%",
            "table-name": "%",
            "column-name": "%"
        }
    }


def create_tasks_for_no_filter_tables(tables):
    """
    Creates JSON files for tables that DO NOT have any filter conditions. Following tables fall under this
    case.
        1. Tables with no filter conditions (E.g., HR.EMPLOYEE)
        2. Schema with all tables (E.g., HR,%)
        3. Tables with "partitions-auto" specified (E.g., HR,EMPLOYEE,partitions-auto)

    Our intention is to create a single DMS task to process all tables that belong a single schema.
    As a result, a single JSON file will be created for a single schema.
    """
    schemas = tables.keys()
    index = 5

    for schema in schemas:
        data = dict()
        data['rules'] = []
        file_name = task_name + '-' + schema + '-all_tables.json'

        for table in tables[schema]:
            logger.debug('Processing table: {}.{}'.format(table.schema, table.table))
            index += 1

            entry = {
                "rule-type": "selection",
                "rule-id": index,
                "rule-name": index,
                "object-locator": {
                    "schema-name": table.schema,
                    "table-name": table.table
                },
                "rule-action": "include",
            }

            # If the table is specified to have have "partitions-auto" in the input csv file
            # create this entry.
            if table.auto_partitioned:
                entry['parallel-load'] = {
                    "type": "partitions-auto"
                }

            data['rules'].append(entry)

        # Add a Transformation
        data['rules'].append(convert_schemas_to_lowercase())
        data['rules'].append(convert_tables_to_lowercase())
        data['rules'].append(convert_columns_to_lowercase())

        with open(os.path.join('json_files', file_name), 'w') as fp:
            json.dump(data, fp)


def create_tasks_for_filter_tables(tables):
    """
    Creates JSON files for tables that DO HAVE any filter conditions.

    One JSON file will be created for each table/condition.
    """
    index = 5

    for table in tables:
        data = dict()
        data['rules'] = []

        logger.debug('Processing table: {}.{}'.format(table.schema, table.table))
        index += 1

        entry = {
            "rule-type": "selection",
            "rule-id": index,
            "rule-name": index,
            "object-locator": {
                "schema-name": table.schema,
                "table-name": table.table
            },
            "rule-action": "include",
        }

        part_of_filename = ''

        # Generate filter conditions
        filter_conditions = []
        for fil in table.filters:
            column = fil.column
            operator = fil.operator.lower()
            value = fil.value

            condition = {}

            if operator == 'between':
                lower, upper = value.split('~')
                upper = upper.strip('\n').strip()

                condition = {
                    "filter-operator": operator,
                    "start-value": lower,
                    "end-value": upper,
                }

                if len(part_of_filename) == 0:
                    part_of_filename = lower + '-' + upper
            else:
                condition = {
                    "filter-operator": operator,
                    "value": value
                }

                if len(part_of_filename) == 0:
                    part_of_filename = value

            filter_condition = {
                "filter-type": "source",
                "column-name": column,
                "filter-conditions": [
                    condition
                ]
            }

            filter_conditions.append(filter_condition)

        entry['filters'] = filter_conditions
        data['rules'].append(entry)

        # Add a Transformation
        data['rules'].append(convert_schemas_to_lowercase())
        data['rules'].append(convert_tables_to_lowercase())
        data['rules'].append(convert_columns_to_lowercase())

        file_name = '{}-{}-{}-{}.json'.format(task_name, table.schema, table.table, part_of_filename)
        file_name = file_name.replace('_', '-')

        with open(os.path.join('json_files', file_name), 'w') as fp:
            json.dump(data, fp)


def print_tables():
    logger.debug('All tables with filter conditions')
    for table in filter_tables:
        logger.debug(table)

    logger.debug('All Tables without filter conditions')
    schemas = non_filter_tables.keys()
    for schema in schemas:
        for table in non_filter_tables[schema]:
            logger.debug(table)


def create_dms_task(task_id, table_mapping):
    """
    Creates AWS Data Migration Task and returns the ARN of created task
    """

    task_arn = ''
    try:
        response = client.create_replication_task(
            ReplicationTaskIdentifier=task_id,
            SourceEndpointArn=config.source_endpoint_arn,
            TargetEndpointArn=config.target_endpoint_arn,
            ReplicationInstanceArn=config.replication_instance_arn,
            MigrationType=config.dms_type,
            TableMappings=table_mapping,
            ReplicationTaskSettings=task_settings
        )
        task_arn = response['ReplicationTask']['ReplicationTaskArn']

    except Exception as error:
        logger.error('Something went wrong while creating Replication task for task_id: {}'.format(task_id))
        logger.error(error)
        print ('Something went wrong while creating Replication task for task_id: ' + task_id + '. Please review output.log for more information.')
        sys.exit(1)

    return task_arn


def process_json_files():
    print ('Creating the DMS tasks:')
    """
    Reads all the json files and generates DMS tasks
    """
    arn_list = []
    count = 0

    # Create tasks for all JSON files        
    for json_file in os.listdir('json_files'):
        if json_file.startswith(task_name):
            file_handler = open(os.path.join('json_files', json_file), 'r')
            table_mapping = json.dumps((json.load(file_handler)))
            file_handler.close()

            task_id = json_file

            # Replace special chars, otherwise AWS will complain.
            task_id = task_id.replace('.json', '').replace('_', '-').replace('.', '-').strip()
            task_arn = create_dms_task(task_id, table_mapping)

            if task_arn != '':
                print('DMS task is being created for file: {}. This may take a few minutes. Please wait.'.format(json_file))
                arn_list.append(task_arn)
            else:
                count += 1

    # Wait for the tasks to be in "READY" state
    wait_for_status_change('replication_task_ready', arn_list)
    print('{} tasks have been created and are ready to be run'.format(len(arn_list)))

    # Delete any old task_arn files from previous runs
    file = 'task_arn'+task_name+'.txt'

    if os.path.isfile(file): 
        os.remove(file)

    # Persist the ARNs in a file.
    with open(task_arn_file, 'w') as file_handle:
        [file_handle.write('%s\n' % arn) for arn in arn_list]

    if count > 0:
        msg = '{} errors encountered while creating DMS tasks. Check the log file.'.format(count)
        print(msg)
        send_mail(msg)
    else:
        send_mail('{} tasks have been created and in ready state'.format(len(arn_list)))


def wait_for_status_change(waiter_state, arn_list):
    """
    Creates waiters for DMS.
    """
    waiter = client.get_waiter(waiter_state)
    waiter.wait(
        Filters=[
            {
                'Name': 'replication-task-arn',
                'Values': arn_list
            },
        ],
    )


def start_dms_tasks():
    count = 0
    arn_list = []

    # Read the task ARNs from "task_arn.txt" file and start the DMS tasks.

    with open(task_arn_file, 'r') as arn_file:
        for arn in arn_file:
            arn = arn.strip('\n')
            arn_list.append(arn)

            try:
                response = client.start_replication_task(
                    ReplicationTaskArn=arn,
                    StartReplicationTaskType='reload-target'
                )
                print('Task: {} has been started'.format(arn))
            except Exception as error:
                count += 1
                logger.error('Error starting task with ARN: {}'.format(arn))

    # Once all the tasks have been started, we wanted to wait for all of them
    # to get completed. that's when their status change to 'replication_task_stopped'.
    # However, there seems to be a bug with this waiter in boto3.
    # https://github.com/boto/boto3/issues/1926
    # wait_for_status_change('replication_task_stopped', arn_list)

    # So, we are simply starting the tasks and return. we are NOT waiting for them
    # to get completed.

    if count > 0:
        msg = '{} errors encountered while starting DMS tasks. Check the log file.'.format(count)
        print(msg)
        send_mail(msg)
    else:
        msg = '{} tasks have been started'.format(len(arn_list))
        print(msg)
        send_mail(msg)


def delete_dms_tasks():
    """
    Delete DMS tasks. The tasks to be deleted come from "task_arn.txt" file.
    """
    count = 0
    arns_to_be_deleted = []
    inputfile = Path(task_arn_file)
    if inputfile.exists():
        with open(task_arn_file, 'r') as arn_file:
            for arn in arn_file:
                arn = arn.strip('\n')
                arns_to_be_deleted.append(arn)

                try:
                    response = client.delete_replication_task(
                        ReplicationTaskArn=arn
                    )
                    print('Task: {} deletion in progress...'.format(arn))
                except Exception as error:
                    count += 1
                    logger.error('Error deleting task with ARN: {}'.format(arn))
    else:
        print ('There is no task arn file for that name. Check it again or create the tasks first calling: calling: "python ' + os.path.realpath(__file__) + ' --create-tasks ' + task_name + '"')
        sys.exit(1)

    if count > 0:
        msg = '{} errors encountered while deleting DMS tasks. Check the log file.'.format(count)
        print(msg)
        send_mail(msg)
    else:
        wait_for_status_change('replication_task_deleted', arns_to_be_deleted)

        msg = '{} tasks have been deleted!'.format(len(arns_to_be_deleted))
        print(msg)
        send_mail(msg)


def create_dms_tasks():
    delete_previous_files()

    # Identify the CSV files and process them
    inputfile = Path(config.csv_tables_location)

    if inputfile.exists():
        for file in os.listdir(config.csv_tables_location):
                process_csv_file(file, "include")
            elif file.startswith("exclude"):
                process_csv_file(file, "exclude")
    else:
        msg = 'Could not locate the csv files to be loaded in the ' + config.csv_tables_location + ' directory'
        print(msg)
        send_mail(msg)

    # Print all tables
    print_tables()

    # Create tasks in JSON form
    create_tasks_for_no_filter_tables(non_filter_tables)
    create_tasks_for_filter_tables(filter_tables)

    logger.debug('JSON files have been created')

    check_existing_tasks()

    # Create Replication tasks
    process_json_files()


def check_existing_tasks():
    # Check to make sure DMS tasks with the same name do not already exist
    for json_file in os.listdir('json_files'):
        if json_file.startswith(task_name):
            task_id = json_file

         # Replace special chars, otherwise AWS will complain.
            task_id = task_id.replace('.json', '').replace('_', '-').replace('.', '-').strip()
            try:
                response = client.describe_replication_tasks(Filters=[
                  {
                     'Name': 'replication-task-id',
                 'Values': [
                     task_id,
                 ]
                },
            ],
                MaxRecords=100,
                Marker='')

                if response != '':
                    print ('Task ID: ' + task_id + ' already exists. Please give a different name for your task or run these existing tasks by calling: "python ' + os.path.realpath(__file__) + ' --run-tasks ' + task_name + '"')
                    sys.exit(1)

            except Exception as error:
                logger.error('No existing tasks found, continuing')




def list_dms_tasks():
    """
    List DMS tasks. The tasks to be listed come from "task_arn.txt" file.
    """
    inputfile = Path(task_arn_file)

    if inputfile.exists():
        with open(task_arn_file, 'r') as arn_file:
            for arn in arn_file:
                arn = arn.strip('\n')
                try:
                    response = client.describe_replication_tasks(Filters=[
                    {
                    'Name': 'replication-task-arn',
                    'Values': [
                     arn,
                    ]
                    },
                    ],
                    MaxRecords=100,
                    Marker='')
                    for task in response['ReplicationTasks']:
                        err_msg = ''

                        if 'LastFailureMessage' in task.keys():
                            err_msg = task['LastFailureMessage']

                        print('Name: {0:30} ARN: {1:20}     Status: {2:30}'.format(task['ReplicationTaskIdentifier'],task['ReplicationTaskArn'], task['Status'], err_msg))

                except Exception as error:
                    logger.error('Error listing task with ARN: {}'.format(arn))
                    print ('There was an error listing tasks. Please check manually.')

    else:
        print ('There is no task arn file for that name. Check it again or create the tasks first calling: calling: "python ' + os.path.realpath(__file__) + ' --create-tasks ' + task_name + '"')
        sys.exit(1)


def send_mail(message):
    try:
        if len(sns_topic_arn) > 0:
            sns.publish(TopicArn=sns_topic_arn, Message=message, )
    except Exception as exception:
        None


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == '--run-tasks':
        start_dms_tasks()
        sys.exit(0)

    if len(sys.argv) == 3 and sys.argv[1] == '--delete-tasks':
        delete_dms_tasks()
        sys.exit(0)

    if len(sys.argv) == 3 and sys.argv[1] == '--list-tasks':
        list_dms_tasks()
        sys.exit(0)

    if len(sys.argv) == 3 and sys.argv[1] == '--create-tasks':
        logger.debug('{0:25} : {1:40}'.format('CSV File', config.csv_tables_location))
        logger.debug('{0:25} : {1:40}'.format('replication_task_settings', config.replication_task_settings))
        logger.debug('{0:25} : {1:40}'.format('replication_instance_arn', config.replication_instance_arn))
        logger.debug('{0:25} : {1:40}'.format('source_endpoint_arn', config.source_endpoint_arn))
        logger.debug('{0:25} : {1:40}'.format('target_endpoint_arn', config.target_endpoint_arn))

        create_dms_tasks()
        sys.exit(0)

    print('Check the config.py file contains the required parameters')
    print('Usage: python dms_task_creator.py [--create-tasks | --run-tasks | --delete-tasks | --list-tasks] task-name')
    sys.exit(0)
