from multiprocessing import Pool
from bs4 import BeautifulSoup
import pandas as pd
import re
import requests
import pickle
import time

############ PATHS ##############

# path to whole texts
b_path = "data/jerome corpus/jerome_synv9_id.txt"

category = "all"

# Out file with only lemmas
lemma_file = "data/lem/{category}/jerome_synv9_publi_no_names.txt".format(category = category)

# File with names 
names_file = "data/names_publi.txt"


############ REGEX ##############

# pattern that finds end of header
header_end = r'syn="\d+" id="[^"]+"' 

############# CODE #############

params = {'data' : 'data',
              'input': 'vertical',
              'output': 'vertical'} 

names_frame = {'title': [], 
               'author': [],
               'type' : [],
               'name entity': []}


time_sleep = 0.6


def process_block(text, names, name_type_tuple_list ):
    global params
    try:
        params['data'] = text
        response = requests.get(url="http://lindat.mff.cuni.cz/services/nametag/api/recognize", params=params)
        response.raise_for_status()  # raises exception when not a 2xx response
        if response.status_code != 204:
            result = response.json()['result']
            result = result.replace("\n", "\t")
            s = result.split('\t')
            s = s[0:-1]
            if len(s) > 1:
                for i in range(0, len(s), 3):
                    names.add(s[i + 2].strip())
                    name_type_tuple_list.append((s[i + 2].strip(), s[i + 1]))
                    print("name: " + s[i + 2])
        time.sleep(time_sleep)            
    except Exception as e:
        print("Error:", e)

    return names, name_type_tuple_list    

    
def main():
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
                        start_time = time.time()
                        
                        for inside in document:
                            if inside['txtype'] not in ['NOV: próza', 'COL: kratší próza', 'MEM: memoáry, autobiografie']:
                                names = set()
                                output_file.write("<doc title=\"" + inside['title'] + "\" author=\"" + inside['author'] + "\" publisher=\"" + inside['publisher'] \
                                    +"\" first_published=\"" + inside['first_published'] + "\" authsex=\"" + inside['authsex'] \
                                    + "\" translator=\""+ inside['translator'] + "\" srclang=\""+ inside['srclang']\
                                    + "\" txtype=\""+ inside['txtype'] + "\" audience=\"" + inside['audience']+"\">\n")
                            
                                
                                blocks = inside.find_all('block')
                                blocks = [block.text for block  in blocks]
                                names = set()
                                author = str(inside['author'])
                                title = str(inside['title'])
                                names_unique = set()
                                name_type_tuple_list = []
                                with Pool() as pool:
                                    names_and_tuple_list = pool.starmap(process_block, [(block, names, name_type_tuple_list) for block in blocks])
                                    for name_set,name_type in  names_and_tuple_list:
                                        names_unique.update(name_set)
                                        for tup in name_type:
                                            names_frame['author'].append(author)
                                            names_frame['title'].append(title)
                                            names_frame['name entity'].append(tup[0])
                                            names_frame['type'].append(tup[1])
                                        
                                # for block in blocks:
                                #     names.update(process_block(block, author, title, names))
                                for text in blocks:
                                    split = text.split('\n')
                                    for line in split:
                                        s = line.split()
                                        if len(s) > 2:
                                            name = s[0]
                                            lemma = s[2]
                                            if category == 'all' and name not in names_unique:
                                                output_file.write(lemma + ' ')
                                output_file.write("\n")
                                output_file.write("\n</doc>\n") 
                                end_time = time.time()
                                print(inside['title'] + ' : ' + str(end_time - start_time))   

                        pickle.dump(names_frame, open('ner_publi.obj', 'wb'))          
                previous_chunk = data_chunk 
    return names


if __name__ == '__main__':
    main()            
    pickle.dump(names_frame, open('ner_publi.obj', 'wb'))
    df = pd.DataFrame(names_frame)  
    df.to_excel("data/names_publi.xlsx")    
