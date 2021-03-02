csv_tables_location = '/Users/kagawron/Documents/DMS_automation/' #Path to folder holding csv
replication_task_settings = 'task_settings.json'
replication_instance_arn = 'arn:aws:dms:eu-west-2:919405152227:rep:HSIC7PFQCBXCSE7WTMEKVQYPBU3Z4B2RE4YM2QQ'
source_endpoint_arn = 'arn:aws:dms:eu-west-2:919405152227:endpoint:K4UXSYTCTSLFULY5QJ7IKCQXPLIUMLMEGDLOG5I'
target_endpoint_arn = 'arn:aws:dms:eu-west-2:919405152227:endpoint:U6G3AZ3SG3HEZMGFK3K6C52O4FZ6IZDMD3NHFHA'
dms_type = 'full-load' #'full-load-cdc', 'full-load' or 'cdc' can be entered
max_lob_size = '' #set to zero if you are not migrating any LOBs
#Optional settings to override default AWSCLI settings
profile = 'default'
region='eu-west-2'
#Optional sns topic connectivity
sns_topic_arn = ''