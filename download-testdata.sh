#!/bin/bash

mkdir -p testdata

curl -L -o testdata/hsk_words.txt https://raw.githubusercontent.com/plar/go-adaptive-radix-tree/master/test/assets/hsk_words.txt 
curl -L -o testdata/uuid.txt https://raw.githubusercontent.com/plar/go-adaptive-radix-tree/master/test/assets/uuid.txt
curl -L -o testdata/words.txt https://raw.githubusercontent.com/plar/go-adaptive-radix-tree/master/test/assets/words.txt

# fetch DOI 2011 list
curl -L -o testdata/DOI-2011.csv.gz https://archive.org/download/doi-urls/2011.csv.gz

# decompress, extract second column and clean up (only unique lines starting with http)
gzip -d -c testdata/DOI-2011.csv.gz | awk -F "," '{print $2}' | grep "^http" | awk '!x[$0]++' > testdata/DOI-2011.txt