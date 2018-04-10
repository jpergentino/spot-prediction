
FOLDER=$1

if [ -z "$1" ]
then
  echo "Parameter required: Source Folder"
  exit
fi

COUNT=0

for f in $FOLDER/*.txt
do 
  filename="$FOLDER/_$(basename ${f/.txt/})"
  echo "Creating file $filename"
  gnuplot -e "output_file='$filename.png'" -e "input_file='$f'" plot.gnu &
  ((COUNT++))
done

echo "Done with $COUNT files."