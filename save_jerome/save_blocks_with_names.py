from bs4 import BeautifulSoup
import re
import pickle
import pandas as pd


############ PATHS ##############

# path to whole texts
b_path = "data/jerome corpus/jerome_synv9_id.txt"

category = "all"

# Out file with only lemmas
lemma_file = "data/lem/{category}/jerome_synv9_all_blocks_with_names.txt".format(category = category)

# File with names 
names_file = "data/names_publi.txt"


############ REGEX ##############

# pattern that finds end of header
header_end = r'syn="\d+" id="[^"]+"' 


time_sleep = 0.6


def main(dict_stats):
    chunk_size = 1024*10000  # Adjust this value based on your needs
    end = False
    i_chunk = 0
    previous_chunk =''
    with open(b_path, 'r', encoding='utf-8') as file:
        with open(lemma_file, "w", encoding="utf8") as output_file:
            while not end:
                last_header_end = -1
                last_header_begin = -1
                data_chunk = ''
                while last_header_end == -1 or last_header_end<=last_header_begin:
                    data_chunk += file.read(chunk_size)
                    i_chunk += 1
                    if not data_chunk:
                        end = True
                        break  # Reached the end of the file
                    # Finding the position of the last '<' character before the first occurrence of 'doc title="'
                    last_header_begin = data_chunk.rfind('<doc title="')
                    
                    # Using regular expression to find all matches of the pattern in the string
                    matches = re.finditer(header_end, data_chunk)

                    for match in matches:
                        last_header_match = match

                    if last_header_match:
                        # Extracting the index of the '>' sign after the last matched pattern
                        last_header_end = last_header_match.end() + data_chunk[last_header_match.end():].index('>')
                    else:
                        last_header_end = -1    

                # Finding the position of the first '<' character before 'doc title="'

                first_header_begin = data_chunk.find('<doc title="')
                
                previous_chunk += data_chunk[0:first_header_begin]
                
                data_chunk = data_chunk[first_header_begin:]
                
                if previous_chunk:
                    
                    x = 2 if end else 1
                    for _ in range(x):
                        # Parse the XML chunk
                        Bs_data = BeautifulSoup(data_chunk, 'lxml')
                        
                        # Find the desired XML tags within the chunk
                        document = Bs_data.find_all('doc')
                        
                        for inside in document:
                            #if inside['txtype'] not in ['NOV: próza', 'COL: kratší próza', 'MEM: memoáry, autobiografie']:
                                output_file.write("<doc title=\"" + inside['title'] + "\" author=\"" + inside['author'] + "\" publisher=\"" + inside['publisher'] \
                                    +"\" first_published=\"" + inside['first_published'] + "\" authsex=\"" + inside['authsex'] \
                                    + "\" translator=\""+ inside['translator'] + "\" srclang=\""+ inside['srclang']\
                                    + "\" txtype=\""+ inside['txtype'] + "\" audience=\"" + inside['audience']+"\">\n")
                                dict_stats['title'].append(inside['title'])
                                dict_stats['author'].append(inside['author'])
                                dict_stats['publisher'].append(inside['publisher'])
                                dict_stats['first_published'].append(inside['first_published'])
                                dict_stats['authsex'].append(inside['authsex'])
                                dict_stats['translator'].append(inside['translator'])
                                dict_stats['srclang'].append(inside['srclang'])
                                dict_stats['txtype'].append(inside['txtype'])
                                dict_stats['audience'].append(inside['audience'])

                                
                                print(inside['author'] + ' : ' +  inside['title'])
                            
                                
                                blocks = inside.find_all('block')
                                blocks = [block.text for block  in blocks]

                                tokens = 0
                                words = 0

                                # for block in blocks:
                                #     names.update(process_block(block, author, title, names))
                                for text in blocks:
                                    split = text.split('\n')
                                    for line in split:
                                        s = line.split()
                                        if len(s) > 2:
                                            lemma = s[2]
                                            tokens += 1
                                            if lemma.isalnum():
                                                words += 1
                                            output_file.write(lemma + ' ')
                                    output_file.write("\n")

                                dict_stats['tokens'].append(tokens)
                                dict_stats['words'].append(words)


                                output_file.write("\n</doc>\n")        
                previous_chunk = data_chunk 
    return dict_stats


if __name__ == '__main__':
    dict_stats = {'title': [],
                  'author': [],
                  'publisher':[],
                  'first_published': [],
                  'authsex':[],
                  'translator': [],
                  'srclang':[],
                  'txtype':[],
                  'audience':[],
                  'tokens':[],
                  'words':[]}
    dict_stats = main(dict_stats)    
    df = pd.DataFrame(dict_stats)
    df.to_excel('data/books info/stats/jerome_all_books_info.xlsx')          
