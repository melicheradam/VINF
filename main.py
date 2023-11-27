from src import crawler, parser, indexer
import argparse

OPERATORS = ["=", ">", "<"]

HELP_STR = """
    Enter command: 
    \t c - crawl
    \t p - parse (html)
    \t sp - spark parse (xml)
    \t i - index           
    \t si - spark index (index spark-parsed documents)
    \t s - search
    \t q - quit
"""

def main():
    argparser = argparse.ArgumentParser(description='A simple script with command-line arguments.')

    # Add arguments
    argparser.add_argument('-c', '--command', type=str, help=HELP_STR)
    argparser.add_argument('-t', '--term', type=str, help='If command was "s", this is used to pass the search term to program. (field=xyz) or (field>123)')


    # Parse the arguments
    args = argparser.parse_args()

    if args.command == "s" and not args.term:
        raise ValueError("When using search, a --term argument is required.")


    while True:

        if not args.command:
            print(HELP_STR)
            command = input("Command: ")
        else:
            command = args.command

        if command == "q":
            break
        elif command == "c":
            crawler.crawl_from('List_of_largest_known_stars')
        elif command == "p":
            parser.clean_htmls()
        elif command == "sp":
            parser.clean_xml_spark()
        elif command == "i":
            indexer.index_cleaned_data()
        elif command == "si":
            indexer.index_cleaned_spark_data()
        elif command == "s":
            while True:
                if not args.command:
                    search_term = input("Enter what to search (field=xyz) or (field>123): ")
                else:
                    search_term = args.term

                if search_term:
                    field, term, operator = None, None, None
                
                    for operator in OPERATORS:
                        opidx = search_term.find(operator)
                        if opidx != -1:
                            field, term = search_term[:opidx], search_term[opidx + 1:]
                            indexer.search_indexed_data(field, term, operator)
                            break
                else:
                    break

                if args.command:
                    break
        elif command == "q":
            break
        
        if args.command:
            break

if __name__ == '__main__':
    main()
