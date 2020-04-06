#!/bin/bash 

export JSON_DIRECTORY=$(dirname $0) 

if [ -z "$ES_SERVER" ]
then
      echo "ES_SERVER env variable is needed to run"
      exit 0
fi
set -e 

#creating the  templates for metrics and directories
curl -XPUT -u "$ES_USER:$ES_PASSWORD" -H 'Content-Type: application/json' -d @$JSON_DIRECTORY/json/metrics_template.json $ES_SERVER/_template/biggraphite_metrics?pretty
curl -XPUT -u "$ES_USER:$ES_PASSWORD" -H 'Content-Type: application/json' -d @$JSON_DIRECTORY/json/directories_template.json $ES_SERVER/_template/biggraphite_directories?pretty

exit 0