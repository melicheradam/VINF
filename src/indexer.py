import lucene
import glob
import os
lucenevm = lucene.initVM(vmargs=['-Djava.awt.headless=true'])

from org.apache.lucene.analysis.en import EnglishAnalyzer
from org.apache.lucene.search import IndexSearcher

from org.apache.lucene.index import IndexWriter, IndexWriterConfig,DirectoryReader
from org.apache.lucene.queryparser.classic import QueryParser

from org.apache.lucene.document import Document, Field, TextField
from org.apache.lucene.store import FSDirectory

from java.io import File

os.makedirs("data/index", exist_ok=True)

print(lucenevm, "lucene version: ", lucene.VERSION)

_index_directory = FSDirectory.open(File("data/index/").toPath())

def index_cleaned_data():

    files = glob.glob("data/parsed/*")
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
            doc.add(Field("NAME", fname, TextField.TYPE_STORED))
            writer.addDocument(doc)
        
        print(f"Indexed {idx + 1} out of {total} files", end="\r")
    
    print(f"Indexed {idx + 1} out of {total} files")

    writer.commit()
    writer.close()


def search_indexed_data(query):
    # searching

    searcher = IndexSearcher(DirectoryReader.open(_index_directory))

    analyzer = EnglishAnalyzer()

    query = QueryParser("TEXT", analyzer).parse(query)
    scoreDocs = searcher.search(query, 5).scoreDocs
    print(f"{len(scoreDocs)} total matching documents. Showing top three documents:")
    for scoreDoc in scoreDocs[:3]:
        doc = searcher.doc(scoreDoc.doc)
        print(f"\nDocument name: {doc.get('NAME')}\nScore: {scoreDoc.score}\nContent: {doc.get('TEXT')[:100]}...")
