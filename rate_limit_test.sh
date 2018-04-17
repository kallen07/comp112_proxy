#  $1 = number of trials to run
#  $2 = file containing list of files to use as samples 

timestamp=`date +%m-%d_%H%M`
for ((j=0;j<$1;j++))
do
    for i in 1000000000,1500000000 100000000,150000000 10000000,15000000 1000000,1500000; do 
        IFS=',' read rate cap <<< "${i}"
        echo "${rate}" bandwidth and "${cap}" burst rate, iteration $j
        while read -r file
        do
            echo $file            
            echo "File ${file}, sample $j, bandwidth= ${rate} burst rate= ${cap}" >> ${timestamp}_${rate}_${cap}.dat

            `python -m SimpleHTTPServer >& /dev/null & `
            sleep 1
            echo Started http server
            `python proxy.py 8080 -r -v --bandwidth=${rate} --burst_rate=${cap} >& proxy_${timestamp}_${file}_${j}.log & `
            sleep 2
            echo Started proxy
    
            /usr/bin/time -a -o ratelimit_times_${file}_${timestamp}_${rate}_${cap}.dat -p curl --proxy localhost:8080 localhost:8000/$file > /dev/null
            echo Done with curl, killing proxy now
            killall python
        done < "$2"
    done
done

for i in 1000000000,1500000000 100000000,150000000 10000000,15000000 1000000,1500000; do 
    IFS=',' read rate cap <<< "${i}"
    echo "${rate}" bandwidth and "${cap}" burst rate
    echo "${rate}" bandwidth and "${cap}" burst rate >> final_ratelimit_time_report_${timestamp}.txt
    while read -r line
    do
        echo $line >> final_ratelimit_time_report_${timestamp}.txt
        
        filename=ratelimit_times_${line}_${timestamp}_${rate}_${cap}.dat
        echo "Calculating mean and stdev for ${line} (bw=${rate}, burst=${cap}"
        ./get_avg_time.sh ${filename} >> final_ratelimit_time_report_${timestamp}.txt
    done < "$2"
done

cat final_ratelimit_time_report_${timestamp}.txt

mkdir timing_stats_ratelimit_${timestamp}
`mv ratelimit_times_*.dat timing_stats_ratelimit_${timestamp}`
`cp final_ratelimit_time_report_${timestamp}.txt timing_stats_ratelimit_${timestamp}`
