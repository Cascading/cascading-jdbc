#!/bin/bash -e
if [[ -z "$1" ]] ||  [[ -z "$2" ]] || [[ -z "$3" ]]
then
  echo "needed args are JDBC url, user and password"
  exit 1
fi

REDSHIFT_URL="$1"
REDSHIFT_USER="$2"
REDSHIFT_PASSWORD="$3"

gradle :cascading-jdbc-redshift:clean :cascading-jdbc-redshift:sampleCode -Dcascading.jdbc.url.redshift="${REDSHIFT_URL}?user=${REDSHIFT_USER}&password=${REDSHIFT_PASSWORD}" -x test

export LINGUAL_PLATFORM=hadoop
CATALOG_PATH=/user/$USER/.lingual
LINGUAL_COMMAND="lingual"

set +e
if hadoop fs -test -e $CATALOG_PATH ; then
  hadoop fs -rmr /user/$USER/.lingual
fi
set -e

echo ""
$LINGUAL_COMMAND catalog --init
echo ""
$LINGUAL_COMMAND catalog --provider --add ./cascading-jdbc-redshift/build/libs/lingual-redshift-sample.jar
echo ""
$LINGUAL_COMMAND catalog  --schema example --add
echo ""
$LINGUAL_COMMAND catalog  --schema example --stereotype creatures --add --columns ID,SPECIES,LOCATION,NAME,TIMESEEN --types int,string,string,string,date
echo ""
$LINGUAL_COMMAND catalog  --schema example --format postgresql --add --provider redshift
echo ""
$LINGUAL_COMMAND catalog  --schema example --protocol jdbc --add --properties="tabledesc.tablename=results2,tabledesc.columnnames=id:species:location:name:timeseen,tabledesc.columndefs=int:varchar(30):varchar(30):varchar(30):date,jdbcuser=${REDSHIFT_USER},jdbcpassword=${REDSHIFT_PASSWORD}"  --provider redshift
echo ""
$LINGUAL_COMMAND catalog  --schema example --format postgresql --add --properties="tabledesc.tablename=results2,tabledesc.columnnames=id:species:location:name:timeseen,tabledesc.columndefs=int:varchar(30):varchar(30):varchar(30):date,jdbcuser=${REDSHIFT_USER},jdbcpassword=${REDSHIFT_PASSWORD}"  --provider redshift
echo ""
$LINGUAL_COMMAND catalog  --schema example --table results2 --stereotype creatures --add ${REDSHIFT_URL}  --protocol jdbc  --format postgresql
echo ""
echo "SELECT * FROM \"example\".\"results2\";" | $LINGUAL_COMMAND shell
