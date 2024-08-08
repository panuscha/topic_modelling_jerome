from bs4 import BeautifulSoup
import pandas as pd
import re
import requests

############ PATHS ##############

# path to whole texts
b_path = "data/jerome corpus/jerome_synv9_id.txt"

category = "all"

# Out file with only lemmas
lemma_file = "data/lem/{category}/jerome_synv9_id_no_names.txt".format(category = category)

# File with names 
names_file = "data/names.txt"


############ REGEX ##############

# pattern that finds end of header
header_end = r'syn="\d+" id="[^"]+"' 

############# CODE #############

parameters = {'data' : 'data',
              'input': 'vertical',
              'output': 'vertical'} 



names_frame = {'title': [], 
               'author': [],
               'type' : [],
               'name entity': []}

chunk_size = 1024*10000  # Adjust this value based on your needs
end = False

names = set()

i_chunk = 0

previous_chunk =''
with open(b_path, 'r', encoding='utf-8') as file:
    with open(lemma_file, "w", encoding="utf8") as output_file, open(names_file, "w", encoding="utf8") as name_file:
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
                        names = set()
                        output_file.write("<doc title=\"" + inside['title'] + "\" author=\"" + inside['author'] + "\" publisher=\"" + inside['publisher'] \
                            +"\" first_published=\"" + inside['first_published'] + "\" authsex=\"" + inside['authsex'] \
                            + "\" translator=\""+ inside['translator'] + "\" srclang=\""+ inside['srclang']\
                            + "\" txtype=\""+ inside['txtype'] + "\" audience=\"" + inside['audience']+"\">\n")
                        
                        name_file.write("<doc title=\"" + inside['title'] + "\" author=\"" + inside['author'] + "\" publisher=\"" + inside['publisher'] \
                            +"\" first_published=\"" + inside['first_published'] + "\" authsex=\"" + inside['authsex'] \
                            + "\" translator=\""+ inside['translator'] + "\" srclang=\""+ inside['srclang']\
                            + "\" txtype=\""+ inside['txtype'] + "\" audience=\"" + inside['audience']+"\">\n")

                        for text in inside.find_all('block'):
                            parameters['data'] = text.text
                            try:
                                response = requests.get(url = "http://lindat.mff.cuni.cz/services/nametag/api/recognize", params=parameters)
                                result = response.json()['result'] 
                                result = result.replace("\n", "\t")
                                s = result.split('\t') 
                                s = s[0:-1]
                                if len(s) > 1 : #and (s[1] == 'pf' or s[1] == 'ps') 
                                    for i in range(0, len(s), 3):
                                        names.add(s[i + 2])

                                        names_frame['author'] = inside['author']
                                        names_frame['title'] = inside['title']
                                        names_frame['type'] = s[i+1]
                                        names_frame['name entity'] = s[i+2].strip()

                                        name_file.write(s[i+2] + "\n") 
                                        print("name: " + s[i+2]) 
                            except:
                                print("Text: \n" + text.text)

                            split = text.text.split('\n')
                            for line in split:
                                s = line.split()
                                if len(s) > 2:
                                    name = s[0]
                                    lemma = s[2] 
                                    if category == 'all' and name not in names: # 
                                        output_file.write(lemma + ' ')
                            output_file.write("\n")
                            output_file.write("\n</doc>\n")   
                        
            previous_chunk = data_chunk     

df = pd.DataFrame(names_frame)  
df.to_excel("data/names.xlsx")             

