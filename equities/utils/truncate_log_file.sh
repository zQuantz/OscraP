line=$(eval "grep -nr SCRAPER,JOB,INITIATED scraper.log | cut -d : -f 1 | tail -1")
total_lines=$(eval "wc -l scraper.log | cut -d ' ' -f 1")
tail="$(($total_lines - $line + 1))"
rm log.log
tail -$tail scraper.log > log.log
