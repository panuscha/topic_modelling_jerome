import pandas as pd
import pickle
import math
import re
from bs4 import BeautifulSoup

def load_books_chunks_from_document(CONST, file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()

    books = []
    books_info = []
    start_tag = '<doc title="'
    end_tag = '</doc>'
    start_index = 0

    while True:
        book_start = content.find(start_tag, start_index)
        if book_start == -1:
            break

        book_end = content.find(end_tag, book_start)
        if book_end == -1:
            break

        book_text = content[book_start:book_end + len(end_tag)]
        book_info = book_text.strip()[5:book_text.index('>') + 1]  # Remove '<doc' and '</doc>'

        book_info_list = book_info.split('" ')
        book_info_dict = {}

        for item in book_info_list:
            key, value = item.split('=')
            book_info_dict[key.strip()] = value.strip('"')

        book_content = book_text[book_text.index('>') + 1:-len(end_tag)].strip()
        book_content = book_content.split(' ')
        length = len(book_content)
        for i in range(math.ceil(length/CONST)):
            i = i*CONST
            end = i+CONST if i+CONST < length-1 else length-1
            books.append(" ".join(book_content[i:end]))
            books_info.append(book_info_dict)

        start_index = book_end + len(end_tag)

    return books, books_info


def load_books_from_document(file_path, txtype_select):
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()

    books = []
    books_info = []
    start_tag = '<doc title="'
    end_tag = '</doc>'
    start_index = 0

    while True:
        book_start = content.find(start_tag, start_index)
        if book_start == -1:
            break

        book_end = content.find(end_tag, book_start)
        if book_end == -1:
            break

        book_text = content[book_start:book_end + len(end_tag)]
        book_info = book_text.strip()[5:book_text.index('>') + 1]  # Remove '<doc' and '</doc>'  # Remove '<doc' and '</doc>'

        
        if txtype_select == [] or any([txtype in book_info for txtype in txtype_select]):
            book_info_list = book_info.split('" ') 
            book_info_dict = {}

            for item in book_info_list:
                key, value = item.split('=')
                book_info_dict[key.strip()] = value.strip('"')

            book_content = book_text[book_text.index('>') + 1:-len(end_tag)].strip()        
            books.append(book_content)

            books_info.append(book_info_dict)

        start_index = book_end + len(end_tag)

    return books, books_info



def load_books_blocks_from_document(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()

    books = []
    books_info = []
    start_tag = '<doc title="'
    end_tag = '</doc>'
    start_index = 0

    while True:
        book_start = content.find(start_tag, start_index)
        if book_start == -1:
            break

        book_end = content.find(end_tag, book_start)
        if book_end == -1:
            break

        book_text = content[book_start:book_end + len(end_tag)]
        book_info = book_text.strip()[5:book_text.index('>') + 1]  # Remove '<doc' and '</doc>'  # Remove '<doc' and '</doc>'

        book_info_list = book_info.split('" ')
        book_info_dict = {}

        for item in book_info_list:
            key, value = item.split('=')
            book_info_dict[key.strip()] = value.strip('"')

        book_content = book_text[book_text.index('>') + 1:-len(end_tag)].strip()
        book_content = book_content.split('\n')
        for book_block in book_content:
            books.append(book_block)
            books_info.append(book_info_dict)

        start_index = book_end + len(end_tag)

    return books, books_info


def load_books_from_document_without_residual_ner(file_path, txtype_select, df_dict_ner):
    if isinstance(df_dict_ner, str):
        with open(df_dict_ner, 'rb') as f:
            names_frame = pickle.load(f)
            df_ner = pd.DataFrame(names_frame)
        df_dict_ner = load_all_names(df_ner)     

    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()

    books = []
    books_info = []
    start_tag = '<doc title="'
    end_tag = '</doc>'
    start_index = 0

    while True:
        book_start = content.find(start_tag, start_index)
        if book_start == -1:
            break

        book_end = content.find(end_tag, book_start)
        if book_end == -1:
            break

        book_text = content[book_start:book_end + len(end_tag)]
        book_info = book_text.strip()[5:book_text.index('>') + 1]  # Remove '<doc' and '</doc>'  # Remove '<doc' and '</doc>'

        
        if txtype_select == [] or any([txtype in book_info for txtype in txtype_select]):
            book_info_list = book_info.split('" ') 
            book_info_dict = {}

            for item in book_info_list:
                key, value = item.split('=')
                book_info_dict[key.strip()] = value.strip('"')
                

            book_content = book_text[book_text.index('>') + 1:-len(end_tag)].strip().split(' ')
            dict_key = (book_info_dict['title'],book_info_dict['author'] )
            if dict_key in df_dict_ner.keys():
                book_content = ' '.join([i for i in book_content if i not in df_dict_ner[(book_info_dict['title'],book_info_dict['author'] )]])  
            else:
                book_content = ' '.join(book_content)    
            books.append(book_content)


            books_info.append(book_info_dict)

        start_index = book_end + len(end_tag)

    return books, books_info

def get_pos(file, txtype_list, work_pos, df_names):
    chunk_size = 1024*10000  # Adjust this value based on your needs
    end = False
    i_chunk = 0
    previous_chunk =''
    # pattern that finds end of header
    header_end = r'syn="\d+" id="[^"]+"'    

    i = 0
    ret_pos = {}
    with open(file, 'r', encoding='utf-8') as file:
        
        
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
                       
                        if txtype_list == [] or  inside['txtype'] in txtype_list:
                            print(i)
                            blocks = inside.find_all('block')
                            blocks = [block.text for block  in blocks]

                                    
                            # for block in blocks:
                            #     names.update(process_block(block, author, title, names))
                            dict_key = (inside['author'], inside['title']) 

                            if dict_key in work_pos:
                                ret_pos[(i, dict_key[0], dict_key[1])] = work_pos[dict_key]

                            else:
                                ret_pos[(i, dict_key[0], dict_key[1])] = []
                                names = df_names[(df_names['title'] == inside['title'])& (df_names['author'] == inside['author'])]['name entity']
                                for text in blocks:
                                    split = text.split('\n')
                                    for line in split:
                                        s = line.split()
                                        if len(s) > 5:
                                            origf = s[0]
                                            if origf not in names:
                                                morf_tag = s[5] 
                                                ret_pos[(i, dict_key[0], dict_key[0])].append(morf_tag)
                            i += 1 

            previous_chunk = data_chunk 
    return ret_pos


def load_all_names(df_ner):
    df_dict_ner={}
    for _, df in df_ner.groupby(['title','author']):
        title = df.title.unique()[0]
        author = df.author.unique()[0]
        # get rid of strings that are too short 
        df_dict_ner[(title, author)] = set(filter(lambda x: len(x)>2,df['name entity'].unique().tolist()))
        
    return df_dict_ner   

def discard_words(word_list, documents):
    # based on: https://stackoverflow.com/questions/48352745/most-efficient-way-to-remove-specific-words-from-a-string 
    
    remove_words = '|'.join(word_list)
    ret_documents = []
    for document in documents:
        ret_documents.append(re.sub(remove_words, '', document))
    return ret_documents

def trim_documents(books, max_length):
    books_ret = []
    for book in books:
        books_ret.append(' '.join([element for element in  book.split()[:max_length]]))
    return books_ret    


if __name__ == '__main__':
    get_pos( "data/jerome corpus/jerome_synv9_id.txt", txtype_list = [])          
