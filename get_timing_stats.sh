rm magistream_times.log 
rm neopets_times.log 
rm bbc_times.log
rm commencement_times.log 

echo "Getting magistream homepage 10 times..."
for i in 1 2 3 4 5 6 7 8 9 10; do
	echo i
	/usr/bin/time -a -o magistream_times.log -p curl --proxy localhost:8080 magistream.com > /dev/null
done
echo "Getting neopets homepage 10 times..."
for i in 1 2 3 4 5 6 7 8 9 10; do
	echo i
	/usr/bin/time -a -o neopets_times.log -p curl --proxy localhost:8080 neopets.com > /dev/null
done
echo "Getting bbc homepage 10 times..."
for i in 1 2 3 4 5 6 7 8 9 10; do
	echo i
	/usr/bin/time -a -o bbc_times.log -p curl --proxy localhost:8080 bbc.com > /dev/null
done
echo "Getting commencement page 10 times..."
for i in 1 2 3 4 5 6 7 8 9 10; do
	echo i
	/usr/bin/time -a -o commencement_times.log -p curl --proxy localhost:8080 http://commencement.tufts.edu/ > /dev/null
done

./get_avg_time.sh magistream_times.log > magistream_avg_time
./get_avg_time.sh neopets_times.log > neopets_avg_time
./get_avg_time.sh bbc_times.log > bbc_avg_time
./get_avg_time.sh commencement_times.log > commencement_avg_time