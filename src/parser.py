#from pyspark.sql import SparkSession

#import findspark
#findspark.init()

# building the Spark session
#spark = SparkSession.builder.master("local[2]").config("spark.executor.memory","512m").getOrCreate()
#print(spark)

import os
import glob
import re

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import length, regexp_replace, regexp_extract, col

import findspark
findspark.init()

# building the Spark session
spark: SparkSession = SparkSession.builder.master("local[*]").config("spark.executor.memory","512m").getOrCreate()

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


def clean_xml_spark():
    
    #print(spark)
    #print(spark.sparkContext.getConf().getAll())
    spark.sparkContext.setLogLevel('WARN')

    files = glob.glob("data/xmls/*")
    total = len(files)

    for idx, file in enumerate(files):

        # loading the XML data
        df: DataFrame = spark.read.text(file, lineSep="<page>")

        df.show(2, truncate=False)

        # extract starbox and infobox and title
        df = df.withColumn('starbox', regexp_extract('value', r"\{\{\s*Starbox begin\s*}}([\s\S]*)\{\{\s*Starbox end\s*}}", 1))
        df = df.withColumn('infobox', regexp_extract('value', r"\{\{\s*Infobox ([\s\S]*)}}\n'''\w*'''[^\n]", 1))
        df = df.withColumn('title', regexp_extract('value', r"<title>(.*?)</title>", 1))



        # extract first section
        df = df.withColumn("value", regexp_extract("value",  r"\n('''.*?)\n", 1))
        df = df.filter(length(col("value")) > 0)
 
        # RegEx expressions for substitutions (text cleaning)
        brackets_regex = r"\{\{.*?}}|\[\[|\]\]"
        references_regex = r"&lt;ref(.*?)&lt;/ref&gt;"
        quote_regex = r"&quot;"
        lt_gt_regex = r"&lt;.*?&gt;"
        amp_regex = r'&amp;'
        pipe_regex = r'\|'
        remaining_special_chars_regex = r'[^A-Za-z0-9\-\. ]+'

        # apply RegEx substitutions to df
        df = df.withColumn('value', regexp_replace('value', brackets_regex, ''))
        df = df.withColumn('value', regexp_replace('value', references_regex, ''))
        df = df.withColumn('value', regexp_replace('value', quote_regex, '\"'))
        df = df.withColumn('value', regexp_replace('value', lt_gt_regex, ''))
        df = df.withColumn('value', regexp_replace('value', amp_regex, ''))
        df = df.withColumn('value', regexp_replace('value', pipe_regex, ' '))
        df = df.withColumn('value', regexp_replace('value', remaining_special_chars_regex, ''))

        #df.show(2, truncate=False)

        #print(df.count())
        #print(type(df))

        # Writing the output into a CSV file
        # repartition(1) to write the output into a single file
        df.repartition(1).write.csv("data/spark-parsed/", mode="append")

        print(f"Parsed {idx + 1} out of {total} files", end="\r")
    
    print(f"Parsed {idx + 1} out of {total} files")


        

