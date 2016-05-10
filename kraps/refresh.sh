while true
do
	scalan-sql/kraps/csv2html q5.csv q5.html
	scp q5.html ws:
	sleep 1
done
