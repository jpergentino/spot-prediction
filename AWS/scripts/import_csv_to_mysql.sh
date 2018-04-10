# Script used to create cases database 
# 
#
# A database with spotprice history is required

# export to CSV = select * from spotprice INTO OUTFILE "/tmp/csv/spotprice.csv" FIELDS TERMINATED BY ';' ENCLOSED BY '"' LINES TERMINATED BY '\n';



FOLDER=$1
TABLE=$2
HOST=
PORT=
export MYSQL_PWD=

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
mysql -h $HOST -P $PORT  -uroot aws -e "TRUNCATE $TABLE"

for f in $FOLDER/*.csv
do 

  echo "Importing $f to $TABLE"
  mysql -h $HOST -P $PORT -u$USER --local-infile aws -e "LOAD DATA LOCAL INFILE '$f' INTO TABLE $TABLE FIELDS TERMINATED BY ';' LINES TERMINATED BY '\n' IGNORE 1 LINES"

done
