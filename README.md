# VINF

Replication commands needed:

```bash

# crawl data (html)
python main.py -c c  

# parse crawled data (html -> txt)
python main.py -c p

# index parsed data (txt -> index)
python main.py -c i

# parse downloaded wiki dump (xml -> csv)
python main.py -c sp 

# extend existing index with parsed data (csv -> index)
python main.py -c si

# searching
python main.py -c s

```

Searching works by specifiend field to search, its operator how to compare values, and the value to search for. Example: field=xyz or field>123 
