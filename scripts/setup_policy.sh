#!/bin/bash 

export JSON_DIRECTORY=$(dirname $0) 

if [ -z "$ES_SERVER" ]
then
      echo "ES_SERVER env variable is needed to run"
      exit 0
fi
set -e 

curl -XPUT -u "$ES_USER:$ES_PASSWORD" -H 'Content-Type: application/json' -d @$JSON_DIRECTORY/json/ilm.json $ES_SERVER/_ilm/policy/biggraphite-metadata?pretty
exit 0