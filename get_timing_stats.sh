#  $1 = file containing sites to time
#  $2 = number of samples to run

# echo "Getting magistream. neopets, bbc, and commencement page 10 times..."
# IFS=$'\n'       # make newlines the only separator
# set -f          # disable globbing

timestamp=`date +%m-%d_%H%M`
for ((i=0;i<$2;i++))
do
    echo "-------------------------------"
    echo "On sample ${i}"
    while read -r line
    do
        filename=${line}_times_${timestamp}_reusing.dat
        echo "Accessing ${line}"
        `python proxy.py 8080 -r -v >& proxy_log_${line}_${timestamp}_${i}_reuse.log & `
        sleep 2
        echo Started proxy with reuse
       
        /usr/bin/time -a -o $filename -p curl --proxy localhost:8080 $line > /dev/null
        echo Done with curl, killing proxy now
        killall python

        filename=${line}_times_${timestamp}_noreuse.dat
        echo "Accessing ${line}"
        `python proxy.py 8080 -v >& proxy_log_${line}_${timestamp}_${i}_noreuse.log & `
        sleep 2
        echo Started proxy with no reuse
       
        /usr/bin/time -a -o $filename -p curl --proxy localhost:8080 $line > /dev/null
        echo Done with curl, killing proxy now
        killall python
    done < "$1"
done 

# for i in 1 2 3 4 5 6 7 8 9 10; do
#   echo i
#   /usr/bin/time -a -o magistream_times_$1.log -p curl --proxy localhost:8080 magistream.com > /dev/null
#   /usr/bin/time -a -o neopets_times_$1.log -p curl --proxy localhost:8080 neopets.com > /dev/null
#   /usr/bin/time -a -o bbc_times_$1.log -p curl --proxy localhost:8080 bbc.com > /dev/null
#   /usr/bin/time -a -o commencement_times_$1.log -p curl --proxy localhost:8080 http://commencement.tufts.edu/ > /dev/null
# done

while read -r line
do
    echo $line >> final_time_report_${timestamp}.txt
    echo "reuse:">> final_time_report_${timestamp}.txt
    filename=${line}_times_${timestamp}_reusing.dat
    echo "Calculating mean and stdev for ${line} (reuse)"
    ./get_avg_time.sh ${filename} >> final_time_report_${timestamp}.txt
     echo "no reuse:">> final_time_report_${timestamp}.txt
    filename=${line}_times_${timestamp}_noreuse.dat
    echo "Calculating mean and stdev for ${line} (no reuse)"
    ./get_avg_time.sh ${filename} >> final_time_report_${timestamp}.txt
done < "$1"

cat final_time_report_${timestamp}.txt

mkdir timing_stats_${timestamp}
`mv proxy_log_*_${timestamp}* timing_stats_${timestamp}`
`mv *_times_${timestamp}*.dat timing_stats_${timestamp}`
`cp final_time_report_${timestamp}.txt timing_stats_${timestamp}`
# ./get_avg_time.sh magistream_times_$1.log > magistream_avg_time
# ./get_avg_time.sh neopets_times_$1.log > neopets_avg_time
# ./get_avg_time.sh bbc_times_$1.log > bbc_avg_time
# ./get_avg_time.sh commencement_times_$1.log > commencement_avg_time

# echo "Average times:" >> average_time_report_$1.txt
# echo "Magistream" >> average_time_report_$1.txt
# cat magistream_avg_time >> average_time_report_$1.txt
# echo "Neopets" >> average_time_report_$1.txt
# cat neopets_avg_time >> average_time_report_$1.txt
# echo "BBC" >> average_time_report_$1.txt
# cat bbc_avg_time >> average_time_report_$1.txt
# echo "Tufts Commencement" >> average_time_report_$1.txt
# cat commencement_avg_time >> average_time_report_$1.txt

# rm magistream_avg_time
# rm neopets_avg_time
# rm bbc_avg_time
# rm commencement_avg_time