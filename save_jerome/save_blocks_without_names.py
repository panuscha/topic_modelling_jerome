from bs4 import BeautifulSoup
import re
import pickle
import pandas as pd


############ PATHS ##############

# path to whole texts
b_path = "data/jerome corpus/jerome_synv9_id.txt"

category = "all"

# Out file with only lemmas
lemma_file = "data/lem/{category}/jerome_synv9_all_blocks_without_residuals_new.txt".format(category = category)

# File with names 
names_file = "data/names_publi.txt"


############ REGEX ##############

# pattern that finds end of header
header_end = r'syn="\d+" id="[^"]+"' 


time_sleep = 0.6



def find_ngram_positions(string, ngrams):
    # Initialize a dictionary to store n-gram positions
    ngram_positions = []
    
    # Iterate over the n-grams
    for ngram in ngrams:
        positions = []
        start_index = 0
        
        # Find the index of the first occurrence of the n-gram in the string
        while True:
            position = string.find(ngram, start_index)
            
            # If the n-gram is found, record its position and update the start index
            if position != -1:
                positions.append(position + 1)  # Adding 1 to match the human-readable position
                start_index = position + 1
            else:
                break
        
        # If any positions were found, add them to the dictionary
        if positions:
            ngram_positions.extend(positions)   
    
    return ngram_positions


def filter_strings_with_multiple_words(string_list):
    return [string for string in string_list if ' ' in string]


def main(df_ner):
    global names_frame
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
                                
                                print(inside['author'] + ' :' +  inside['title'])
                            
                                
                                blocks = inside.find_all('block')
                                blocks = [block.text for block  in blocks]

                                names_unique = set(df_ner[(df_ner['title'] == inside['title']) & (df_ner['author'] == inside['author'])]['name entity'])

                                ngrams = filter_strings_with_multiple_words(names_unique)

                                # for block in blocks:
                                #     names.update(process_block(block, author, title, names))
                                for text in blocks:
                                    dict_name_lemma = {'name':[], 'lemma':[]}
                                    split = text.split('\n')
                                    for line in split:
                                        s = line.split()
                                        if len(s) > 2:
                                            name = s[0]
                                            lemma = s[2]
                                            dict_name_lemma['name'].append(name)
                                            dict_name_lemma['lemma'].append(lemma)

                                    do_not_include = find_ngram_positions(' '.join(dict_name_lemma['name']), ngrams)
                                    for i, lemma in enumerate(dict_name_lemma['lemma']):
                                        name = dict_name_lemma['name'][i]
                                        if name not in names_unique and i not in do_not_include:
                                            output_file.write(lemma + ' ')
                                    output_file.write("\n")
                                output_file.write("\n</doc>\n")        
                previous_chunk = data_chunk 

if __name__ == '__main__':
    print('Loading NER')
    with open('ner_combined.obj', 'rb') as f:
        names_frame = pickle.load(f)
        df_ner = pd.DataFrame(names_frame)

    print('Discarting NER')    
    main(df_ner)                  
