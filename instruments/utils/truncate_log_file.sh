dir=`dirname $0`
dir=`dirname $dir`

line=$(eval "grep -nr Job\ Initiated. $dir/instruments.log | cut -d : -f 1 | tail -1")
total_lines=$(eval "wc -l $dir/instruments.log | cut -d ' ' -f 1")
tail="$(($total_lines - $line + 1))"
rm $dir/log.log
tail -$tail $dir/instruments.log > $dir/log.log
