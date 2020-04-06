#!/bin/bash 

export JSON_DIRECTORY=$(dirname $0) 

if [ -z "$ES_SERVER" ]
then
      echo "ES_SERVER env variable is needed to run"
      exit 0
fi
set -e 

# creating the base index for metrics and directories
# all the rollover indices will use the rollover_alias 
# the write_index will be the last created 
curl -XPUT -u "$ES_USER:$ES_PASSWORD" -H 'Content-Type: application/json' -d @$JSON_DIRECTORY/json/metrics_index.json $ES_SERVER/biggraphite_metrics-000001?pretty
curl -XPUT -u "$ES_USER:$ES_PASSWORD" -H 'Content-Type: application/json' -d @$JSON_DIRECTORY/json/directories_index.json $ES_SERVER/biggraphite_directories-000001?pretty
exit 0