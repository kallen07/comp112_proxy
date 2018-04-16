#  $1 = number of trials to run
#  $2 = file containing list of files to use as samples 

timestamp=`date +%m-%d_%H%M`

for ((j=0;j<$1;j++))
do
    for i in 1000000000,1500000000 100000000,150000000 10000000,15000000 1000000,1500000; do 
        while read -r line
        do
            echo $file
            IFS=',' read rate cap <<< "${i}"
            echo "${rate}" bandwidth and "${cap}" burst rate, iteration $j
            echo "Sample $j" >> ${timestamp}_${rate}_${cap}.dat
            echo "${rate}" bandwidth and "${cap}" burst rate >> ${timestamp}_${rate}_${cap}.dat
            `python -m SimpleHTTPServer >& /dev/null & `
            sleep 1
            echo Started http server
            `python proxy.py 8080 -r -v --bandwidth=${rate} --burst_rate=${cap} >& proxy_${timestamp}_${file}_${j}.log & `
            sleep 2
            echo Started proxy
            echo "Timing results:" >> ${timestamp}_${rate}_${cap}.dat
            `python run_tests.py --num_samples 1 --files $file >& run_tests_output`
            cat run_tests_output >> ${timestamp}_${rate}_${cap}.dat
            sleep 1
            echo Done running script, killing all python now
            killall python
        done < "$1"
    done
done
