import time
import ijson, json
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

def process_large_json_file_v3(file_path, file_start, file_end=9999999999999):
    t1 = time.time()
    location_tweets_count_dict = {'1gsyd':0,'2gmel':0,'3gbri':0,'4gade':0,'5gper':0,'6ghob':0}
    with open(file_path,'rb+') as file:
        print(file.readline())
        print(file.readline())

    return location_tweets_count_dict

def main(filename):
    location_tweets_count_dict = {'1gsyd':0,'2gmel':0,'3gbri':0,'4gade':0,'5gper':0,'6ghob':0}
    t1 = time.time()
    # load twitter data
    with open(filename, 'rb') as f:
        objects = ijson.items(f, 'item')
        # in case only one node ############
        num_objects = 0
        end = 0
        for obj in objects:
            num_objects += 1
            place = obj["includes"]['places'][0]['full_name']
            gcc = compare_city_name(place)
            if gcc in location_tweets_count_dict.keys():
                location_tweets_count_dict[gcc]+=1
    print(time.time()-t1)
    return sorted(location_tweets_count_dict.items(), key=lambda x:x[1], reverse=True)

filename='smallTwitter.json'
# main(filename)
process_large_json_file_v3(filename, 0)