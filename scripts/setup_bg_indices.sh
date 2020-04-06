#!/bin/bash 

if [ -z "$ES_SERVER" ]
then
      echo "ES_SERVER env variable is needed to run"
      exit 1
fi

JSON_DIRECTORY=$(dirname $0) 

set -e
# creating the policy 
curl -XPUT -u "$ES_USER:$ES_PASSWORD" -H 'Content-Type: application/json' -d @$JSON_DIRECTORY/json/ilm.json $ES_SERVER/_ilm/policy/biggraphite-metadata?pretty

#creating the  templates for metrics and directories
curl -XPUT -u "$ES_USER:$ES_PASSWORD" -H 'Content-Type: application/json' -d @$JSON_DIRECTORY/json/metrics_template.json $ES_SERVER/_template/biggraphite_metrics?pretty
curl -XPUT -u "$ES_USER:$ES_PASSWORD" -H 'Content-Type: application/json' -d @$JSON_DIRECTORY/json/directories_template.json $ES_SERVER/_template/biggraphite_directories?pretty

# creating the base index for metrics and directories
# all the rollover indices will use the rollover_alias 
# the write_index will be the last created 

curl -XPUT -u "$ES_USER:$ES_PASSWORD" -H 'Content-Type: application/json' -d @$JSON_DIRECTORY/json/metrics_index.json $ES_SERVER/biggraphite_metrics-000001?pretty
curl -XPUT -u "$ES_USER:$ES_PASSWORD" -H 'Content-Type: application/json' -d @$JSON_DIRECTORY/json/directories_index.json $ES_SERVER/biggraphite_directories-000001?pretty
exit 0