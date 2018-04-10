# Script used to create cases database 
# 
#
# A database with spotprice history is required

# export MySQL to CSV = select * from spotprice INTO OUTFILE "/tmp/csv/spotprice.csv" FIELDS TERMINATED BY ';' ENCLOSED BY '"' LINES TERMINATED BY '\n';


FOLDER=$1
TABLE=$2
DATABASE=aws
USER=
HOST=
export PGPASSWORD=


if [ -z "$1" ]
then
  echo "Parameter required: Source Folder"
  exit
fi

if [ -z "$2" ]
then
  echo "Parameter required: Table destination"
  exit
fi

echo "Cleaning table $TABLE"
psql -h $HOST -U $USER -c "TRUNCATE $TABLE" aws

for f in $FOLDER/*.csv
do 

  DATE=`date '+%Y-%m-%d %H:%M:%S'`
  echo "[$DATE] Importing $f to $TABLE"

  psql -h $HOST -U $USER -c "COPY $TABLE FROM '$f' WITH DELIMITER ';' CSV HEADER;" $DATABASE &

done
