#!/bin/bash

if [[ $# -lt 2 ]]; then
	echo "Invalid arguments!"
	echo "Usage:"
	echo "$0 <firts year> <last year>"
	exit
fi

YearBegin=$1
YearEnd=$2

OutputDirectory=./input/temperatures/${YearBegin}-${YearEnd}

mkdir -p ${OutputDirectory}

for Year in $(seq ${YearBegin} ${YearEnd}); do
	
	echo "Downloading temperature index file for year ${Year}..."

	URL=https://www1.ncdc.noaa.gov/pub/data/noaa/${Year}

	wget ${URL} -O files.txt -nv

	Lines=`html2text files.txt | tail -n +5 | wc -l`

	let numFiles=${Lines}-1

	files=`html2text files.txt | tail -n +5 | head -n ${numFiles} | tr -s " " | cut -d ' ' -f 3`

	for file in ${files}; do
		wget ${URL}/${file} -O ${OutputDirectory}/${file} -nv		
	done
	
	rm files.txt
done
