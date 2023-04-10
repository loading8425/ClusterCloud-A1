import json, ijson ,re
import os, math
import time, queue
from collections import Counter, defaultdict
import heapq

# load sal data
sal_filename = "sal.json"
with open(sal_filename, 'r') as f:
    sal_data = json.load(f)

def compare_city_name(place: str):
    p = place.lower().split(', ')
    if p[0] in sal_data.keys():
        gcc = sal_data[p[0]]["gcc"]
        return gcc
    elif len(p) > 1:
        if p[1] in sal_data.keys():
            gcc = sal_data[p[1]]["gcc"]
            return gcc
        if p[1]=='melbourne' or p[1]=='victoria':
            p[0] = p[0]+' (vic.)'
        elif p[1]=='new south wales':
            p[0] = p[0]+' (nsw)'
        elif p[1]=='queensland':
            p[0] = p[0]+' (qld)'
        elif p[1]=='south australia':
            p[0] = p[0]+' (sa)'
        elif p[1]=='Western Australia':
            p[0] = p[0]+' (wa)'
            
        if p[0] in sal_data.keys():
            gcc = sal_data[p[0]]['gcc']
            return gcc
    return None

def tweet_processing(tweet, counters):
    author_id = tweet['data']['author_id']
    counters[0][author_id]+=1
    place_name = tweet['includes']['places'][0]['full_name']
    gcc = compare_city_name(place_name)
    if gcc:
        if not re.match(r'^.[r].*', gcc):
            counters[1][gcc]+=1
            counters[2][author_id][gcc] += 1


def find_next_start_json(file_path, start):
    with open(file_path, 'rb') as file:
            # Move the starting point to the next complete JSON object
            file.seek(start)
            while True:
                line = file.readline()
                if not line or line==b'':
                    return -1
                try:
                    tmp = line.decode().strip().split(':')[0]
                except:
                    print("error:",line, file.tell(),"####", os.path.getsize(file_path))
                if tmp == "\"_id\"":
                    break
            i=0
            pre_line = file.tell()-len(line)
            file.seek(pre_line)
            while True:
                file.seek(file.tell()-2)
                i+=1
                if file.read(1) == b'{':
                    break
            start = pre_line - i - 1
            file.seek(start)
            if file.read(1) != b'{':
                print("error:", line)
    return start

def read_one_json_obj(file_path, start)->list:
    if start <= -1:
        return None, -1
    start = find_next_start_json(file_path, start)
    with open(file_path,'r',encoding='utf-8') as file:
        file.seek(start)
        buffer = ''
        object_count = 0
        while True:
            line = file.readline()
            if not line:
                break
            if line.strip().split(':')[0] == "\"_id\"":
                object_count+=1
                if object_count == 2:
                    break
            buffer += line
        i=0
        for c in buffer[::-1]:
            if c=='}':
                break
            i+=1
        buffer = buffer[:len(buffer)-i]
        js=json.loads(buffer)
        end = file.tell()-len(line) - 4
    return js, end

# slower json read but no edit needed
def process_large_json_file_v2(file_path, file_start, file_end=9999999999999):
    filesize = os.path.getsize(file_path)
    file_end=file_end if file_end < filesize else filesize
    location_tweets_count= Counter()
    id_count = Counter()
    id_count_with_location = defaultdict(Counter)
    counters = [id_count,location_tweets_count, id_count_with_location]

    if file_start < 0:
        file_start = 0
    if file_start >= filesize:
        return None
    with open(file_path,'rb') as file:
        start = find_next_start_json(file_path, file_start)
        file.seek(start)
        stop = False
        num_objects = 0
        line = b's\n'
        while not stop:
            buffer = b''
            # load next one json object
            while True:
                line = file.readline()
                buffer += line
                if line == b'':
                    break
                #print(line)
                try:
                    if line == b'  },\n' or line == b',\n':
                        # delete unwanted chars at end of json data
                        tmp = buffer.decode('UTF-8')
                        i=0
                        for c in tmp[::-1]:
                            if c=='}':
                                break
                            i+=1
                        tmp = tmp[:len(tmp)-i]
                        data = json.loads(tmp)
                        break
                except:
                    pass
            last_obj = data["_id"]
            
            # print number of data
            num_objects += 1
            # print_data = "% Number of data processed: "+str(num_objects)+"current _id:"+last_obj
            # print("\r{:3}".format(print_data),end=' ')

            # processing tweets data
            tweet_processing(data, counters)
            # if end of file
            end = file.tell()
            if end >= file_end-1000:
                break
    return counters

# fastest processing with ijson, but file editing is needed
def process_large_json_file_v3(file_path, file_start, file_end=9999999999999):
    filesize = os.path.getsize(file_path)
    if(file_start < 0):
        file_start = 0
    location_tweets_count_dict = {'1gsyd':0,'2gmel':0,'3gbri':0,'4gade':0,'5gper':0,'6ghob':0}
    id_count = Counter()
    visited_tweets = []
    start = find_next_start_json(file_path, file_start)
    if start == -1:
        return location_tweets_count_dict
    with open(file_path,'rb+') as file:
        #print(start)
        # add '[' at begining of start point for ijson read convenience
        file.seek(start-1)
        file.write(b'[')
        file.seek(start-2)
        objects = ijson.items(file, 'item')
        num_data = 0
        for data in objects:
            if num_data<1:
                visited_tweets.append([data["_id"], start])
            
            # print number of data
            # print_data = "Processing...."+str(round(file.tell() / filesize, 3)*100)+"% Number of data processed: "+str(num_data)
            # print("\r{:3}".format(print_data),end=' ')

            # if loop over file end add last json object to our results
            if file.tell() >= file_end:
                last_obj = data["_id"]
                place = data["includes"]['places'][0]['full_name']
                gcc = compare_city_name(place)
                if gcc in location_tweets_count_dict.keys():
                    location_tweets_count_dict[gcc]+=1
                    id_count.update([data['data']['author_id']])
                visited_tweets.append([last_obj, file.tell()])
                break
            num_data+=1
            # processing data
            if type(data)!=type(dict()):
                print(data[0],'\n',data[-1])
            place = data["includes"]['places'][0]['full_name']
            gcc = compare_city_name(place)
            if gcc in location_tweets_count_dict.keys():
                location_tweets_count_dict[gcc]+=1
                id_count.update([data['data']['author_id']])
                #print(data['data']['author_id'])

            # remove added '[' at begining of the file seek
            if num_data==1:
                curr = file.tell()
                file.seek(start-1)
                file.write(b' ')
                file.seek(curr)
    #print(num_data)
    return location_tweets_count_dict, visited_tweets, id_count

#***#
# loop through all results from each node or core
# find whether last end json object at one core is correct  
# find each ignored json objects
# edit results
#***#
def data_Validation(filename,data, results, id_count):
    pre_end_data = data[0][1]
    for rank in data:
        if rank == 0:
            continue

        #print(data[rank])
        start_data = data[rank][0]
        start_id = start_data[0]
        start = pre_end_data[1] - 75535
        count = 0
        flag = False

        # find this whether this data is duplicated
        duplicate_falg = False
        if pre_end_data[0] == start_id:
            #print('pre end id == start id')
            duplicate_falg = True

        while True:
            tmp = start
            obj,start = read_one_json_obj(filename, start)
            if obj['_id'] == pre_end_data[0]:
                #print(obj['_id'], start, "search start:", tmp, "pre end id:",pre_end_data[0])
                flag = True
                if duplicate_falg:
                    #print("duplicated found")
                    place = obj["includes"]['places'][0]['full_name']
                    gcc = compare_city_name(place)
                    if gcc in results.keys():
                        results[gcc]-=1
                        id_count[obj['data']['author_id']] -= 1
                    break
            elif flag:
                count += 1
                if obj['_id'] == start_id:
                    #print(obj['_id'], start, "search start:", tmp, "pre end id:",pre_end_data[0], 'count', count)
                    break
                else:
                    place = obj["includes"]['places'][0]['full_name']
                    gcc = compare_city_name(place)
                    if gcc in results.keys():
                        results[gcc]+=1
                        id_count[obj['data']['author_id']] += 1
        pre_end_data = data[rank][1]
    return results, id_count

# for test purpose
if __name__ == '__main__':
    t1 = time.time()
    file_path = 'smallTwitter.json'
    start = 0
    ans = process_large_json_file_v2(file_path, 0)
    #print(ans)
    print(time.time()-t1)