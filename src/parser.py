#from pyspark.sql import SparkSession

#import findspark
#findspark.init()

# building the Spark session
#spark = SparkSession.builder.master("local[2]").config("spark.executor.memory","512m").getOrCreate()
#print(spark)

import os
import glob
import re

ELEMENT_REGEX_MAP = {
    'h1': r'<h1.*?>(.*?)</h1>',
    'h2': r'<h2.*?>(.*?)</h2>',
    'h3': r'<h3.*?>(.*?)</h3>',
    'p': r'<p.*?>(.*?)</p>',
    #'ul': r'<ul .*?>(.*?)</ul>',
    #'table': r'<table .*?>(.*?)</table>',
}

def clean_htmls():

    files = glob.glob("data/htmls/*")
    total = len(files)

    for idx, file in enumerate(files):
        fname = os.path.basename(file).replace(".html", "")
        with open(file, "r") as f:
            data = f.read()

        cleaned = []

        for el, reg in ELEMENT_REGEX_MAP.items(): # filter out only wanted tags            
            cleaned += re.findall(reg, data, re.DOTALL)

        cleaned_text = "".join(cleaned)
        cleaned_text = re.sub(r'<[^>]*>', ' ', cleaned_text) # remove all tags
        cleaned_text = re.sub(r'&#[0-9]*;', '', cleaned_text)
        cleaned_text = re.sub(r'&amp;', '', cleaned_text)
        cleaned_text = re.sub(r'\[  edit  \]', '', cleaned_text)
        
        with open(f"data/parsed/{fname}.txt", "w") as f:
            f.write(cleaned_text)
        
        print(f"Parsed {idx + 1} out of {total} files", end="\r")
    
    print(f"Parsed {idx + 1} out of {total} files")

        

