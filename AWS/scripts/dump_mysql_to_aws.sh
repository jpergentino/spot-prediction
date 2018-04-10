# Script used to dump local MySQL database
# to aws MySQL database. 
#
# A database with cases table history is required

export HOST_ORIGEM=
export PORTA_ORIGEM=
export DATABASE_ORIGEM=
export TABLE_ORIGEM=

export HOST_DESTINO=
export PORTA_DESTINO=
export DATABASE_DESTINO=

export BKP_NAME="_backup"

echo "Doing a cases table backup $HOST_DESTINO at `date` from $TABLE_ORIGEM to $TABLE_ORIGEM$BKP_NAME"
mysql -h $HOST_DESTINO -P $PORTA_DESTINO -u $USER -p$PASSWORD $DATABASE_DESTINO -e 'ALTER TABLE $TABLE_ORIGEM RENAME TO $TABLE_ORIGEM$BKP_NAME';
echo "Done at `date`."

#echo "Press ENTER to continue..."

echo "Migrating table $TABLE_ORIGEM from $HOST_ORIGEM to $HOST_DESTINO at `date`"
mysqldump -h $HOST_ORIGEM -P $PORTA_ORIGEM -u $USER -p$PASSWORD $DATABASE_ORIGEM $TABLE_ORIGEM | mysql -h $HOST_DESTINO -P $PORTA_DESTINO -u pergentino -pPergenta $DATABASE_DESTINO
echo "Done at `date`."