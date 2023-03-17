import json, ijson
import os, math
import time, queue
# load sal data
sal_filename = "sal.json"
with open(sal_filename, 'r') as f:
    sal_data = json.load(f)

def process_data(data):
    pass

def compare_city_name(place: str):
    p = place.lower().split(',')
    if p[0] in sal_data.keys():
        gcc = sal_data[p[0]]["gcc"]
        return gcc
    elif len(p) > 1:
        if p[1]==' melbourne' or p[1]==' victoria':
            p[0] = p[0]+' (vic.)'
        elif p[1]==' new south wales':
            p[0] = p[0]+' (nsw)'
        elif p[1]==' queensland':
            p[0] = p[0]+' (qld)'
        elif p[1]==' south australia':
            p[0] = p[0]+' (sa)'
            
        if p[0] in sal_data.keys():
            gcc = sal_data[p[0]]['gcc']
            return gcc
    return -1

def find_next_start_json(file_path, start):
    with open(file_path, 'r', encoding='utf-8') as file:
            # Move the starting point to the next complete JSON object
            file.seek(start)
            while True:
                line = file.readline()
                if not line:
                    return -1
                tmp = line.strip().split(':')[0]
                if tmp == "\"_id\"":
                    break
            i=0
            for c in line[::-1]:
                if c=='{':
                    break
                i+=1
            start = file.tell() - i - 3
    return start

# def read_one_json_obj(file_path, start)->dict:
#     start = find_next_start_json(file_path, start)
#     if start == -1:
#         return None, -1
#     with open(file_path,'r',encoding='utf-8') as file:
#         file.seek(start)
#         buffer = ''
#         object_count = 0
#         while True:
#             line = file.readline()
#             if not line:
#                 break
#             if line.strip().split(':')[0] == "\"_id\"":
#                 object_count+=1
#                 if object_count == 2:
#                     break
#             buffer += line
#         i=0
#         for c in buffer[::-1]:
#             if c=='}':
#                 break
#             i+=1
#         buffer = buffer[:len(buffer)-i]
#         js=json.loads(buffer)
#         end = file.tell()-len(line) - 4
#     return js, end

# def read_json_file(file_path, file_start, file_end):
#     dic_list = []
#     start = file_start
#     end = file_start
#     num_obj = 0
#     while True:
#         objs,end = read_one_json_obj(file_path, start)
#         if objs==None:
#             break
#         dic_list.append(objs)
#         if end>=file_end:
#             break
#         num_obj+=1
#         start = end
#     #print(dic_list)
#     #print(len(dic_list))
#     return dic_list, end

# def process_large_json_file(file_path, file_start, file_end=9999999999999):
#     location_tweets_count_dict = {'1gsyd':0,'2gmel':0,'3gbri':0,'4gade':0,'5gper':0,'6ghob':0}
#     with open(file_path,'r',encoding='utf-8') as file:
#         start = find_next_start_json(file_path, file_start)
#         file.seek(start)
#         stop = False
#         while not stop:
#             buffer = ''
#             object_count = 0
#             # load next one json object
#             while True:
#                 line = file.readline()
#                 if not line:
#                     stop = True
#                     break
#                 if line.strip().split(':')[0] == "\"_id\"":
#                     object_count+=1
#                     if object_count == 2:
#                         break
#                 buffer += line
#             # delete unwanted chars at end of json data
#             i=0
#             for c in buffer[::-1]:
#                 if c=='}':
#                     break
#                 i+=1
#             buffer = buffer[:len(buffer)-i]
#             #print(buffer)
#             #print("##############################################")
#             data=json.loads(buffer)
#             # processing data
#             place = data["includes"]['places'][0]['full_name']
#             gcc = compare_city_name(place)
#             if gcc in location_tweets_count_dict.keys():
#                 location_tweets_count_dict[gcc]+=1
#             end = file.tell()-len(line) - 4
#             if end >= file_end:
#                 break
#             file.seek(end)
#     return location_tweets_count_dict

# def process_large_json_file_v2(file_path, file_start, file_end=9999999999999):
#     location_tweets_count_dict = {'1gsyd':0,'2gmel':0,'3gbri':0,'4gade':0,'5gper':0,'6ghob':0}
#     with open(file_path,'r',encoding='utf-8') as file:
#         start = find_next_start_json(file_path, file_start)
#         file.seek(start)
#         stop = False
#         bracket_queue = queue.LifoQueue()
#         while not stop:
#             buffer = ''
#             # load next one json object
#             while True:
#                 line = file.readline()
#                 if not line:
#                     stop = True
#                     break
#                 tmp_line = line.strip()
#                 if tmp_line[0] == ',':
#                     continue
#                 if tmp_line[0] == '{' or tmp_line[-1] == '{':
#                     bracket_queue.put('{')
#                 elif tmp_line[0] == '}' or (tmp_line[-1] == '}' and tmp_line[-2] != '{'):
#                     bracket_queue.get() 
#                     if bracket_queue.qsize() == 0:
#                         buffer += line
#                         break
#                 buffer += line
#             # delete unwanted chars at end of json data
#             i=0
#             for c in buffer[::-1]:
#                 if c=='}':
#                     break
#                 i+=1
#             buffer = buffer[:len(buffer)-i]
#             if stop:
#                 break
#             try:
#                 data=json.loads(buffer)
#             except:
#                 print(buffer)
#                 print("##############################################")
#                 return
#             # processing data
#             place = data["includes"]['places'][0]['full_name']
#             gcc = compare_city_name(place)
#             if gcc in location_tweets_count_dict.keys():
#                 location_tweets_count_dict[gcc]+=1
#             end = file.tell()
#             if end >= file_end:
#                 break
#     return location_tweets_count_dict

def process_large_json_file_v3(file_path, file_start, file_end=9999999999999):
    location_tweets_count_dict = {'1gsyd':0,'2gmel':0,'3gbri':0,'4gade':0,'5gper':0,'6ghob':0}
    start = find_next_start_json(file_path, file_start)
    if start == -1:
        return location_tweets_count_dict
    with open(file_path,'rb+') as file:
        #print(start)
        file.seek(start)
        file.write(b'[')
        file.seek(start)
        objects = ijson.items(file, 'item')
        num_data = 0
        last_obj = {}
        first_obj = {}
        rs = file.tell()
        for data in objects:
            num_data+=1
            if num_data==1:
                first_obj = data
            # processing data
            place = data["includes"]['places'][0]['full_name']
            gcc = compare_city_name(place)
            last_obj = data
            if gcc in location_tweets_count_dict.keys():
                location_tweets_count_dict[gcc]+=1
            end = file.tell()
            if end >= file_end:
                break
        #print(num_data)
        print(last_obj['_id'],first_obj['_id'], "end at:", file.tell(), "file_start:",file_start, "start",start)
        file.seek(start)
        file.write(b' ')
    return location_tweets_count_dict

# for test purpose
if __name__ == '__main__':
    file_path = 'twitter-data-small.json'
    # calculate chunk for each nodes
    file_size = os.path.getsize(file_path)
    chunk_size = math.ceil(file_size / 2)
    print("file size:",file_size)
    start = 0 * chunk_size
    end = min(file_size, start + chunk_size)
    print("\nnode 0 : ",start, end)
    print(process_large_json_file_v3(file_path, start,end))

    start = 1 * chunk_size
    end = min(file_size, start + chunk_size)
    print("\nnode 1 : ",start, end)
    print(process_large_json_file_v3(file_path, start,end))


    print("\n",process_large_json_file_v3(file_path, 0))