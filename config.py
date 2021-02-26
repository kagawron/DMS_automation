csv_tables_location = '' #Path to folder holding csv
replication_task_settings = 'task_settings.json'
replication_instance_arn = ''
source_endpoint_arn = ''
target_endpoint_arn = ''
dms_type = '' #'full-load-cdc', 'full-load' or 'cdc' can be entered
#Optional settings to override default AWSCLI settings. Leave blank to use the [default]
profile = ''
region=''
#Optional sns topic connectivity
sns_topic_arn = ''