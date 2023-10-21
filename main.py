from src import crawler, parser, indexer

while True:

    print("""
Enter command: 
\t c - crawl
\t p - parse
\t i - index           
\t s - search
\t q - quit
""")
    command = input("Command: ")

    if command == "q":
        break
    elif command == "c":
        crawler.crawl_from('List_of_largest_known_stars')
    elif command == "p":
        parser.clean_htmls()
    elif command == "i":
        indexer.index_cleaned_data()
    elif command == "s":
        while True:
            search_term = input("Enter what to search: ")

            if search_term:
                indexer.search_indexed_data(search_term)
            else:
                break
    elif command == "q":
        break
    else:
        continue