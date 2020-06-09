line=$(eval "grep -nr Job\ Initiated. ~/OscraP/instruments/instruments.log | cut -d : -f 1 | tail -1")
total_lines=$(eval "wc -l ~/OscraP/instruments/instruments.log | cut -d ' ' -f 1")
tail="$(($total_lines - $line + 1))"
rm ~/OscraP/instruments/log.log
tail -$tail ~/OscraP/instruments/instruments.log > ~/OscraP/instruments/log.log
