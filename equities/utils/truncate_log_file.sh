line=$(eval "grep -nr SCRAPER,JOB,INITIATED ~/OscraP/equities/scraper.log | cut -d : -f 1 | tail -1")
total_lines=$(eval "wc -l ~/OscraP/equities/scraper.log | cut -d ' ' -f 1")
tail="$(($total_lines - $line + 1))"
rm ~/Quant/OscraP/equities/log.log
tail -$tail ~/OscraP/equities/scraper.log > ~/OscraP/equities/log.log
