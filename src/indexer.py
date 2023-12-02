import lucene
import glob
import os
import csv
import re
import sys
import math
csv.field_size_limit(sys.maxsize)

lucenevm = lucene.initVM(vmargs=['-Djava.awt.headless=true'])

from org.apache.lucene.analysis.en import EnglishAnalyzer
from org.apache.lucene.search import IndexSearcher, TermQuery

from org.apache.lucene.index import IndexWriter, IndexWriterConfig,DirectoryReader, Term
from org.apache.lucene.queryparser.classic import QueryParser

from org.apache.lucene.document import Document, Field, TextField, StringField, FloatPoint, StoredField, SortedNumericDocValuesField
from org.apache.lucene.store import FSDirectory
from org.apache.lucene.search import IndexSearcher, Sort, SortField
from org.apache.lucene.util import NumericUtils

from java.io import File

print(lucenevm, "lucene version: ", lucene.VERSION)


def index_cleaned_data(dir_path):
    _index_directory = FSDirectory.open(File(f"{dir_path}/index/").toPath())

    files = glob.glob(f"{dir_path}/parsed/*")
    total = len(files)

    analyzer = EnglishAnalyzer()

    config = IndexWriterConfig(analyzer)
    writer = IndexWriter(_index_directory, config)

    for idx, file in enumerate(files):
        fname = os.path.basename(file)
        with open(file, "r") as f:
            data = f.read()
            doc = Document()
            doc.add(Field("TEXT", data, TextField.TYPE_STORED))
            doc.add(Field("NAME", fname.replace(".txt", ""), StringField.TYPE_STORED))
            doc.add(Field("NAME_NORM", fname.replace(".txt", "").replace("_", "").replace("-", "").lower(), StringField.TYPE_STORED))
            writer.addDocument(doc)
        
        print(f"Indexed {idx + 1} out of {total} files", end="\r")
    
    print(f"Indexed {idx + 1} out of {total} files")

    writer.commit()
    writer.close()


STARBOX_FIELDS_REGEX = {
    "EPOCH": re.compile(r"epoch\s*=[\s\[\{|]*([\w.]*)[]|}]*"),
    "CONSTELLATION": re.compile(r"constell\s*=[\s\[\{|]*([\w.]*)[]|}]*"),
    "TYPE": re.compile(r"type\s*=[\s\[\{|]*([\w.]*)[]|}]*"),
    "MASS": re.compile(r"mass\s*=[\sA-Za-z\-\[\{|]*([\-+\d.]*)[]|}]*"),
    "LUM": re.compile(r"luminosity\s*=[\sA-Za-z\-\[\{|]*([\-+\d.]*)[]|}]*"),
    "TEMP": re.compile(r"temperature\s*=[\sA-Za-z\-\[\{|]*([\-+\d.]*)[]|}]*"),
    "GRAVITY": re.compile(r"gravity\s*=[\sA-Za-z\-\[\{|]*([\-+\d.]*)[]|}]*"),
}


def index_cleaned_spark_data(dir_path):
    _index_directory = FSDirectory.open(File(f"{dir_path}/index/").toPath())

    files = glob.glob(f"{dir_path}/spark-parsed/*.csv")
    total = len(files)

    analyzer = EnglishAnalyzer()

    config = IndexWriterConfig(analyzer)
    writer = IndexWriter(_index_directory, config)

    for idx, file in enumerate(files):

        with open(file, "r") as f:

            csv_data = csv.reader(f)
            searcher = IndexSearcher(DirectoryReader.open(_index_directory))

            for jdx, row in enumerate(csv_data):
                doc_name = row[3].replace("_", "").replace("-", "").lower()
                starbox = row[1]
                infobox = row[2]
                new_text = row[0]
                
                # Retrieve the existing document (if it exists) by the "name" field
                existing_doc_term = Term("NAME_NORM", doc_name)
                q = TermQuery(existing_doc_term)

                existing_doc = searcher.search(q, 1).scoreDocs

                if len(existing_doc):
                    existing_doc = searcher.doc(existing_doc[0].doc)
                else:
                    continue

                existing_text = existing_doc.get("TEXT")
                updated_text = existing_text # + " " + new_text
                doc = Document()
                doc.add(Field("TEXT", updated_text, TextField.TYPE_STORED))
                doc.add(Field("NAME", existing_doc.get("NAME"), StringField.TYPE_STORED))
                doc.add(Field("NAME_NORM", existing_doc.get("NAME_NORM"), StringField.TYPE_STORED))

                if starbox:
                    for field, regex in STARBOX_FIELDS_REGEX.items():
                        result = regex.search(starbox)
                        if result:
                            result = result.group(1)
                            if result:
                                try:
                                    result = float(result)
                                    doc.add(FloatPoint(field, result))
                                    doc.add(StoredField(field, result))
                                    doc.add(SortedNumericDocValuesField(field, NumericUtils.floatToSortableInt(result)))
                                except ValueError:
                                    doc.add(Field(field, result, StringField.TYPE_STORED))

                # Delete the old document (if it exists) and add the updated one
                writer.updateDocument(existing_doc_term, doc)
                print(f"Indexed {jdx + 1} documents", end="\r")

        
        print(f"Indexed {idx + 1} out of {total} files")
    

    writer.commit()
    writer.close()


def search_indexed_data(field, term, operator, dir_path):
    # searching
    _index_directory = FSDirectory.open(File(f"{dir_path}/index/").toPath())

    field = field.upper()
    searcher = IndexSearcher(DirectoryReader.open(_index_directory))

    term = term.strip()
    field = field.strip()

    analyzer = EnglishAnalyzer()
    
    if operator in ["<", ">"]:

        try: 
            term = float(term)
            if operator == "<":
                query = FloatPoint.newRangeQuery(field, sys.float_info.min, term)
            else:
                query = FloatPoint.newRangeQuery(field, sys.float_info.max, term)

        except ValueError:
            query = FloatPoint.newRangeQuery(field, sys.float_info.min, sys.float_info.max)
        finally:
            #sort = Sort(SortField(field, SortField.Type.FLOAT, True))
            scoreDocs = searcher.search(query, 3).scoreDocs

    else:
        query = QueryParser(field, analyzer).parse(term)
        scoreDocs = searcher.search(query, 3).scoreDocs

    print(f"Showing top three documents:")
    for scoreDoc in scoreDocs:
        doc = searcher.doc(scoreDoc.doc)
        fields_str = "\n".join([f"{field}: {doc.get(field)}" for field in STARBOX_FIELDS_REGEX.keys()])
        print(f"\nDocument name: {doc.get('NAME')}\nScore: {scoreDoc.score}\n{fields_str}\nContent: {doc.get("TEXT")[:300]}")
        print("---------------------------------------------------------")
