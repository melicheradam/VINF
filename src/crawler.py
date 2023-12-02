import requests
from bs4 import BeautifulSoup
from collections import defaultdict
import pandas as pd
import time
import re
import glob
import os

BASE_URL = "https://en.wikipedia.org/wiki/"


def load_state(dir_path) -> dict:
    data = {}
    for file in glob.glob(f"{dir_path}/htmls/*"):
        with open(file, "r") as f:
            fname = os.path.basename(file).replace(".html", "")
            data[fname] = f.read()

    return data

def save_state(data: dict, dir_path):

    print(f"Saving state...")

    count = 0

    for fname, html_data in data.items():
        if not os.path.exists(f"data/htmls/{fname}.html"):
            with open(f"{dir_path}/htmls/{fname}.html", "w") as f:
                f.write(html_data)
                count += 1

    print(f"Succesfully stored {count} files")


def crawl_from(seed):
    crawled_links = load_state() # visited links
    buffer = [seed] + list(crawled_links.keys()) # buffer of links to visit
    counts = [] # number of links found at each depth
    
    count = 0
    loop_count = 0

    print(f"Starting to crawl links from page {seed}")
    
    while loop_count <= 5000:
        
        current = buffer.pop(0)

        if current not in crawled_links:
            time.sleep(1)  # politeness policy
            new_links = do_crawl(current, crawled_links)
        else:
            new_links = do_crawl(current, crawled_links, do_request=False)

        for link in new_links:
            if link not in crawled_links:
                buffer.append(link)
                count += 1
        print(f"Crawled {loop_count} pages, buffer contains {len(buffer)} more", end="\r")
        counts.append(count)

        if loop_count % 100 == 0:
            save_state(crawled_links)

        loop_count += 1


def do_crawl(link, crawled_links, do_request=True):
    
    new_links = []

    if do_request:
        response = requests.get(BASE_URL + link)
        raw_data = response.text
        crawled_links[link] = raw_data
    else:
        raw_data = crawled_links[link]

    soup = BeautifulSoup(raw_data, 'html.parser')
    body_content = soup.find('div', {'id': 'mw-content-text'})

    #Removing refrences
    for ref in body_content.find_all('ol', class_='references'):
        ref.decompose()
        

    # this patter match only links that redicrecto to wikipedia, and only until " or # is encountered
    pattern = r'(?<=href="\/wiki\/)[^"#]+'

    for link in re.findall(pattern, str(body_content)):
        if ":" not in link and "/" not in link:
            new_links.append(link)

    return new_links
