value=`sqlfluff lint Queries/* --dialect hive`
if [[ $value == *"FAIL"* ]]; then
fix=`sqlfluff fix Queries/* --force --dialect hive`
echo $fix
remaining_format=`sqlfluff lint Queries/* --dialect hive`
echo $remaining_format
else;
echo 'success';
fi;

