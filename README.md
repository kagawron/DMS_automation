# Automating AWS Data Migration Service (DMS) Tasks - Create, monitor and delete

## Overview
Main objective of this script is to automate DMS task creation by taking a CSV file input containing table configuration. 
The script generates required JSON configuration files that will be fed to DMS service. This allows for improve accuracy of job creation as human error is reduced
and manual task creation using Web console can be avoided which improves efficiency.

## Pre-Requisites
- python installed locally
- boto3 installed
- aws cli IAM user created with permissions for DMS taks creation and viewing
- aws cli configured and a default profile created
- ARN for the DMS endpoints created for the source and target databases
- ARN for the DMS replication instance created

## `Configuration`

### `config.py`
- First configure `config.py` file with ARNs of Source end point, Target endpoint, and Replication Instance.
- The script can optionally publish messages to given SNS topic after tasks have been created, and deleted. If an SNS topic is created, specify the topic's ARN in `sns_topic_arn` attribute. This is optional feature though. If this notification feature is not required, simply leave the empty quotes.  

```python
csv_tables_location = '' #Path to folder holding csv
replication_task_settings = 'task_settings.json' #Task settings file
replication_instance_arn = ''
source_endpoint_arn = ''
target_endpoint_arn = ''
dms_type = '' #'full-load-cdc', 'full-load' or 'cdc' can be entered
#Optional settings to override default AWSCLI settings. Leave blank to use locally defined defaults
profile = ''
region=''
#Optional sns topic connectivity
sns_topic_arn = ''
```

### `include.csv`
```
<SCHEMA>,<TABLE>,[<FILTER_COLUMN>,<FILTER_CONDITION>,<FILTER_VALUE(S)>]
SCOTT,EMP,HIRE_DATE,GTE,2005-01-01
SCOTT,SALGRADE
SCOTT,DEPT
```

****
## Example

### 
```sh
─
PythonEnv % python /Users/kagawron/Documents/DMS_automation/task_creation.py
Check the config.py file contains the required parameters
Usage: python dms_task_creator.py [--create-tasks | --run-tasks | --delete-tasks | --list-tasks] task-name
``` 

```sh
─
PythonEnv % python /Users/kagawron/Documents/DMS_automation/task_creation.py --create-tasks DMSTASK1
Clearing up old files:
Creating the DMS tasks:
DMS task is being created for file: DMSTASK1-SCOTT-EMP-2005-01-01.json. This may take a few minutes. Please wait.
DMS task is being created for file: DMSTASK1-SCOTT-all_tables.json. This may take a few minutes. Please wait.
2 tasks have been created and are ready to be run
```

```sh
─
PythonEnv % python /Users/kagawron/Documents/DMS_automation/task_creation.py --list-tasks DMSTASK1
Name: dmstask1-scott-emp-2005-01-01  ARN: arn:aws:dms:eu-west-2:999999999999:task:ARQULLKS5APOH55WPWQEFJYWL5FMV3CCV7THMLI     Status: ready                         
Name: dmstask1-scott-all-tables      ARN: arn:aws:dms:eu-west-2:999999999999:task:YWHESPEUDCZCE6XR6OGGWPKVJK7M4QIQWMM6HKQ     Status: ready           
```

```sh
─
PythonEnv % python /Users/kagawron/Documents/DMS_automation/task_creation.py --run-tasks DMSTASK1
Task: arn:aws:dms:eu-west-2:999999999999:task:ARQULLKS5APOH55WPWQEFJYWL5FMV3CCV7THMLI has been started
Task: arn:aws:dms:eu-west-2:999999999999:task:YWHESPEUDCZCE6XR6OGGWPKVJK7M4QIQWMM6HKQ has been started
2 tasks have been started
```



```sh
─
PythonEnv % python /Users/kagawron/Documents/DMS_automation/task_creation.py --list-tasks DMSTASK1
Name: dmstask1-scott-emp-2005-01-01  ARN: arn:aws:dms:eu-west-2:999999999999:task:ARQULLKS5APOH55WPWQEFJYWL5FMV3CCV7THMLI     Status: starting                       
Name: dmstask1-scott-all-tables      ARN: arn:aws:dms:eu-west-2:999999999999:task:YWHESPEUDCZCE6XR6OGGWPKVJK7M4QIQWMM6HKQ     Status: starting                               
```

```sh
─
PythonEnv % python /Users/kagawron/Documents/DMS_automation/task_creation.py --list-tasks DMSTASK1
Name: dmstask1-scott-emp-2005-01-01  ARN: arn:aws:dms:eu-west-2:919405152227:task:ARQULLKS5APOH55WPWQEFJYWL5FMV3CCV7THMLI     Status: running                       
Name: dmstask1-scott-all-tables      ARN: arn:aws:dms:eu-west-2:919405152227:task:YWHESPEUDCZCE6XR6OGGWPKVJK7M4QIQWMM6HKQ     Status: running 
```

```sh
─
PythonEnv % python /Users/kagawron/Documents/DMS_automation/task_creation.py --list-tasks DMSTASK1
Name: dmstask1-scott-emp-2005-01-01  ARN: arn:aws:dms:eu-west-2:919405152227:task:ARQULLKS5APOH55WPWQEFJYWL5FMV3CCV7THMLI     Status: stopped                       
Name: dmstask1-scott-all-tables      ARN: arn:aws:dms:eu-west-2:919405152227:task:YWHESPEUDCZCE6XR6OGGWPKVJK7M4QIQWMM6HKQ     Status: stopped 
```

```sh
─
PythonEnv % python /Users/kagawron/Documents/DMS_automation/task_creation.py --delete-tasks DMSTASK1
Task: arn:aws:dms:eu-west-2:919405152227:task:ARQULLKS5APOH55WPWQEFJYWL5FMV3CCV7THMLI deletion in progress...
Task: arn:aws:dms:eu-west-2:919405152227:task:YWHESPEUDCZCE6XR6OGGWPKVJK7M4QIQWMM6HKQ deletion in progress...
2 tasks have been deleted!
```
****
