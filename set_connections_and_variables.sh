#!/bin/bash
airflow users create --email khanhtd1999@gmail.com --firstname Khanh --lastname DK --password admin --role Admin --username admin

airflow connections add aws_credentials \
    --conn-type aws \
    --conn-login AKIAXEUHQLVQMO74HXV3 \
    --conn-password FFXW7iyQ1p085SWZXl4SxrFwY8eIC6JIxC5oSUHu \
    --conn-extra '{"region_name":"us-west-2"}'
airflow connections add redshift --conn-uri 'redshift://admin:Khanh585@udacity-4.490984201568.us-west-2.redshift-serverless.amazonaws.com:5439/dev'
airflow variables set s3_bucket udacity-dend
airflow variables set LOG_DATA log_data
airflow variables set SONG_DATA song_data
airflow variables set LOG_JSONPATH s3://udacity-dend/log_json_path.json
