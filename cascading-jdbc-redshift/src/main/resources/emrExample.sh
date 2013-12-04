#!/bin/bash -e

if [[ -z "$1" ]] ||  [[ -z "$2" ]] || [[ -z "$3" ]] || [[ -z "$4" ]] || [[ -z "$5" ]]
then
  echo "needed args are Redshift JDBC url, user, password, S3 bucket name, availavility zone"
  exit 1
fi

gradle sampleCode

REDSHIFT_URL=$1
REDSHIFT_USER=$2
REDSHIFT_PASSWORD=$3
BUCKET=$4
ZONE=$5

NAME=lingual-redshift-sample.jar
BUILD=build/libs
DATAFILE="sampleData.csv"

echo using AWS_ACCESS_KEY: $AWS_ACCESS_KEY
echo using AWS_SECRET_KEY: $AWS_SECRET_KEY

HDFS_TMP=$BUCKET/tmp

# clear previous output
s3cmd del -r s3://$BUCKET/$DATAFILE
s3cmd del -r s3://$BUCKET/$NAME

s3cmd put src/main/resources/$DATAFILE s3://$BUCKET/$DATAFILE
s3cmd put $BUILD/$NAME s3://$BUCKET/$NAME

# launch cluster and run
elastic-mapreduce --create --name "lingual-redshift-example" \
  --visible-to-all-users \
  --num-instances 1 \
  --slave-instance-type m1.medium \
  --debug \
  --enable-debugging \
  --verbose \
  --availability-zone $ZONE \
  --log-uri s3n://$BUCKET/logs \
  --jar s3n://$BUCKET/$NAME \
  --arg s3n://$BUCKET/$DATAFILE \
  --arg s3n://$HDFS_TMP \
  --arg $REDSHIFT_URL \
  --arg $REDSHIFT_USER \
  --arg $REDSHIFT_PASSWORD \
  --arg $AWS_ACCESS_KEY \
  --arg $AWS_SECRET_KEY

