dir=`dirname $0`
dir=`dirname $dir`

line=$(eval "grep -nr SCRAPER,JOB,INITIATED $dir/scraper.log | cut -d : -f 1 | tail -1")
total_lines=$(eval "wc -l $dir/scraper.log | cut -d ' ' -f 1")
tail="$(($total_lines - $line + 1))"
rm $dir/log.log
tail -$tail $dir/scraper.log > $dir/log.log
