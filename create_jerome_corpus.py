import re 
import pandas as pd

'''
Selects fiction books from corpus SYN9 and divides it into three categories based on source language
author=\""
"<doc title=\""
'''

path = "D:\\Panuskova\\Documents\\syn_v9"

read = False
df_jerome = pd.read_excel('data/jerome_seznam.xlsx',header=0)
df_jerome['pubyear'] = df_jerome['pubyear'].apply(lambda x: str(x)) 


found = {'title' : [], 'author': [], 'pubyear' : [], 'first_published' : [], 'txtype' : [], 'id': [], 'pubyear_syn' : [], 'first_published_syn' : [], 'txtype_syn' : [] }

authors = df_jerome['author']
titles = df_jerome['title']

jerome_path = 'jerome_synv9_id.txt'
count = 0
MAX = 1523

def app(dict_found, line, row):
    dict_found['title'].append(row['title'])
    dict_found['author'].append(row['author'])
    dict_found['pubyear'].append(row['pubyear'])
    dict_found['first_published'].append(row['first_published'])
    dict_found['txtype'].append(row['txtype'])
    dict_found['id'].append(row['id'])
    
    pubyear = re.search('(?<=pubyear=")[0-9]{4}', line)
    first_published = re.search('(?<=first_published=")[0-9]{4}', line)
    txtype = re.search('(?<=txtype=")[A-Z]{3}', line)
    
    if pubyear:
        dict_found['pubyear_syn'].append(pubyear.group())
    else:
        dict_found['pubyear_syn'].append("")    
    
    if first_published:
        dict_found['first_published_syn'].append(first_published.group())
    else:
        dict_found['first_published_syn'].append("")  
    
    if txtype:
        dict_found['txtype_syn'].append(txtype.group())
    else:
        dict_found['txtype_syn'].append("")    
    
    return dict_found


try:
    with open(path, encoding="utf8") as f, open (jerome_path , 'w', encoding="utf8") as jerome:
        for line in f:
            #if "FIC: beletrie"in line and ("NOV: próza" or "COL: kratší próza") in line and "srclang=\"cs: čeština" in line and "GEN: obecné publikum" in line:
            if "<doc title=" in line:
                id = re.search('(?<=id=")[^"]+', line)
                #txtype = re.search('(?<=txtype=")[A-Z]{3}', line)
                #pubyear = re.search('(?<=pubyear=")[0-9]{4}', line)
                #first_published = re.search('(?<=first_published=")[0-9]{4}', line)
                if id :  #and first_published
                    id = id.group()
                    #pubyear = pubyear.group()
                    #first_published = first_published.group()
                    title = re.search('(?<=title=")[^"]+', line)
                    author = re.search('(?<=author=")[^"]+', line)
                    if author:
                        author = author.group()
                    else:
                        author = 'Y'  
                        
                    if title:
                        title = title.group()
                    else:
                        title = 'X'      
                          
                    idx = df_jerome[df_jerome['id']==id].index.values
                    if len(idx) > 0:
                        idx = idx[0] 
                        # if  df_jerome.loc[idx,'author'] == author: #df_jerome.loc[idx,'txtype'] == txtype and df_jerome.loc[idx,'pubyear'] == pubyear and df_jerome.loc[idx,'first_published'] == first_published
                        print("Title: {title}, Author: {author} count = {i}".format(title = title, author = df_jerome.loc[idx,'author'], i = count))
                        found = app(found, line, df_jerome.loc[idx].squeeze())
                        count += 1 
                        read = True
                        jerome.write(line)
                    else:
                        read = False
                else:
                    read = False        
            elif  read :
                    jerome.write(line)
            # if not read : #and count >= MAX
        #         break     
finally:
    df_found = pd.DataFrame(found)
    df_found.to_csv("found_titles_synv9_id.csv")        