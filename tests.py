import os
from unittest import TestCase
import unittest
from src.parser import clean_htmls, clean_xml_spark
import shutil
import glob

SAMPLE_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Sample HTML</title>
</head>
<body>
    <h1>Title of the Page</h1>
    <p>This is the first paragraph.</p>
    <p>This is the second paragraph.</p>
    <p>And here is the third paragraph.</p>
</body>
</html>
"""

def create_dir_structure():
    shutil.rmtree("test-data", ignore_errors=True)
    os.makedirs("test-data/htmls", exist_ok=True)
    os.makedirs("test-data/xmls", exist_ok=True)
    os.makedirs("test-data/parsed", exist_ok=True)
    os.makedirs("test-data/spark-parsed", exist_ok=True)
    os.makedirs("test-data/index", exist_ok=True)


class TestHtmlCleaning(TestCase):

    def setUp(self) -> None:
        create_dir_structure()
        f = open("test-data/htmls/sample.html", "w")
        f.write(SAMPLE_HTML)
        f.close()

    def test_paragraph_extraction(self):
        # Test case for paragraph extraction

        # Expected output text file
        expected_output_file = "test-data/parsed/sample.txt"

        # Run the function to extract paragraphs
        clean_htmls("test-data")

        # Check if the output file has been created
        self.assertTrue(os.path.exists(expected_output_file), "Output file not found.")

        # Check if the content of the output file matches the expected content
        with open(expected_output_file, "r", encoding="utf-8") as expected_file:
            expected_content = expected_file.read()

        self.assertEqual(expected_content, "Title of the PageThis is the first paragraph.This is the second paragraph.And here is the third paragraph.")


SAMPLE_XML = """<?xml version="1.0" encoding="UTF-8"?>
<mediawiki xmlns="http://www.mediawiki.org/xml/export-0.10/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.mediawiki.org/xml/export-0.10/ http://www.mediawiki.org/xml/export-0.10.xsd" version="0.10" xml:lang="en">
  <siteinfo>
    <sitename>Sample Wikipedia</sitename>
    <base>http://sample.wikipedia.org/wiki/Main_Page</base>
    <generator>MediaWiki 1.36.0</generator>
    <case>first-letter</case>
    <namespaces>
      <namespace key="-2" case="first-letter">Media</namespace>
      <namespace key="-1" case="first-letter">Special</namespace>
      <namespace key="0" case="first-letter" />
      <namespace key="1" case="first-letter">Talk</namespace>
      <namespace key="2" case="first-letter">User</namespace>
      <!-- Add more namespaces as needed -->
    </namespaces>
  </siteinfo>
  <page>
    <title>Sample_Article</title>
    <ns>0</ns>
    <id>1</id>
    <revision>
      <id>123456789</id>
      <parentid>0</parentid>
      <timestamp>2023-01-01T12:34:56Z</timestamp>
      <contributor>
        <username>TestUser</username>
        <id>12345</id>
      </contributor>
      <comment>Initial version</comment>
      <model>wikitext</model>
      <format>text/x-wiki</format>
      <text xml:space="preserve">\n'''This is a sample article
      </text>
      <sha1>abcdef1234567890abcdef1234567890abcdef12</sha1>
    </revision>
  </page>
</mediawiki>
"""

class TestXmlCleaning(TestCase):

    def setUp(self) -> None:
        create_dir_structure()
        f = open("test-data/xmls/sample.xml", "w")
        f.write(SAMPLE_XML)
        f.close()

    def test_paragraph_extraction(self):
        # Test case for paragraph extraction

        # Run the function to extract paragraphs
        clean_xml_spark("test-data")

        # Expected output text file
        files = glob.glob(f"test-data/spark-parsed/*.csv")
        
        self.assertEqual(len(files), 1)

        expected_output_file = files[0]

        # Check if the content of the output file matches the expected content
        with open(expected_output_file, "r", encoding="utf-8") as expected_file:
            expected_content = expected_file.read()

        self.assertEqual(expected_content, "This is a sample article,\"\",\"\",Sample_Article\n")


if __name__ == '__main__':
    unittest.main()