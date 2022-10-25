cat small.init | while read line
do
    psql -d small -a -f $line.sql
done