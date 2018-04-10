# Script used to generate cases 
# based on price history database
# 
# CSV files will be created in $FOLDER and used by 
# 'import_csv_to_mysql.sh' script to create database
#
# A database with spotprice history is required

if [ -z "$1" ]
then
  echo "Parameter required: Home destination folder"
  exit
fi

if [ -z "$2" ]
then
  echo "Parameter required: Save on database? true or false"
  exit
fi

PARAMS="-XX:-UseGCOverheadLimit -Xmx9G"
FOLDER="$HOME/$1"
SAVE_DB=$2

cd ..
java $PARAMS -cp .:./*:./libs/* main.CaseCreator $SAVE_DB $FOLDER

echo "Done."