from mpi4py import MPI
import json, ijson, os, math
import re
import threading
from utils import  process_large_json_file_v3, data_Validation, process_large_json_file_v2
import heapq
import time

# Ground Truth
# Processing....100.0% Number of data processed: 9092273 
# [('2gmel', 2286773), ('1gsyd', 2115934), ('3gbri', 857063), ('5gper', 589115), ('4gade', 462676), ('6ghob', 90053)]
# [('1089023364973219840', 28128), ('826332877457481728', 27581), ('1250331934242123776', 25247), ('1423662808311287813', 21030), ('1183144981252280322', 20656), ('820431428835885059', 20063), ('1270672820792508417', 19801), ('233782487', 18179), ('84958532', 15676), ('719139700318081024', 15314)]
# time spend: 130.86579751968384

# json processing with ijson mod(file editing needed)
def ijsonMain():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    filename = "bigTwitter.json"
    top_k = 10
    # calculate chunk for each nodes
    file_size = os.path.getsize(filename)
    chunk_num = math.ceil(file_size / 65536)       # config specifically for ijson
    chunk_num_each_node = int(chunk_num/size)           # each node process chunks with size of 65535
    if chunk_num_each_node < 1:
        chunk_num_each_node = 1
    start = rank * chunk_num_each_node*65536
    end = start + chunk_num_each_node*65537

    #--------------------First Node--------------------#
    if rank == 0:
        print("Program running on size of %d core"% (size))

        if size == 1:
            location_tweets_count_dict,_,id_count = process_large_json_file_v3(filename,0)
            print()
            print(sorted(location_tweets_count_dict.items(), key=lambda x:x[1], reverse=True))
            k_most_common = heapq.nlargest(top_k, id_count.items(), key=lambda x: x[1])
            print(k_most_common)
################################################ multi nodes #############################################
        else:
            location_tweets_count_dict, visited, id_count = process_large_json_file_v3(filename,0,end)
            visited_dict = {rank:visited}
            for r in range(1,size):
                data = comm.recv(source=r)
                visited_list = data[1]
                visited_dict[r] = visited_list
                id_count += data[2]
                for key, value in data[0].items():
                    if key not in data[0].keys():
                        location_tweets_count_dict[key] = value
                    else:
                        location_tweets_count_dict[key] += value
            
            # processing data that is missed or duplicated
            location_tweets_count_dict, id_count = data_Validation(filename,visited_dict, location_tweets_count_dict, id_count)
            location_tweets_count_dict = sorted(location_tweets_count_dict.items(), key=lambda x:x[1], reverse=True)
            print(location_tweets_count_dict,'\n')

            # get top k number of tweets
            k_most_common = heapq.nlargest(top_k, id_count.items(), key=lambda x: x[1])
            print(k_most_common)

    #--------------------Other Node--------------------#
    else:
        location_tweets_count_dict, visited_list, id_count = process_large_json_file_v3(filename,start,end)
        
        #print("Core %d done, total number of objects = %d, results:"% (rank, num_objects), location_tweets_count_dict)
        # send back processed data back to root
        comm.send([location_tweets_count_dict, visited_list, id_count], dest=0)

# json processing without editing file
def jsonMain():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    filename = "bigTwitter.json"
    top_k = 10
    # calulate chunkc for each nodes
    file_size = os.path.getsize(filename)
    chunk_size = math.ceil(file_size / size)
    start = rank * chunk_size
    end = start + chunk_size

    #--------------------First Node--------------------#
    if rank == 0:
        print("Program running on size of %d core"% (size))
        location_tweets_count_dict, visited, id_count = process_large_json_file_v2(filename,0,end)
        print()
        visited_dict = {rank:visited}
        for r in range(1,size):
            data = comm.recv(source=r)
            visited_list = data[1]
            visited_dict[r] = visited_list
            id_count += data[2]
            for key, value in data[0].items():
                if key not in data[0].keys():
                    location_tweets_count_dict[key] = value
                else:
                    location_tweets_count_dict[key] += value
        
        # processing data that is missed or duplicated
        #location_tweets_count_dict, id_count = data_Validation(filename,visited_dict, location_tweets_count_dict, id_count)
        location_tweets_count_dict = sorted(location_tweets_count_dict.items(), key=lambda x:x[1], reverse=True)
        print(location_tweets_count_dict,'\n')

        # get top k number of tweets
        k_most_common = heapq.nlargest(top_k, id_count.items(), key=lambda x: x[1])
        print(k_most_common)

    #--------------------Other Node--------------------#
    else:
        location_tweets_count_dict, visited_list, id_count = process_large_json_file_v2(filename,start,end)
        
        #print("Core %d done, total number of objects = %d, results:"% (rank, num_objects), location_tweets_count_dict)
        # send back processed data back to root
        comm.send([location_tweets_count_dict, visited_list, id_count], dest=0)

if __name__ == '__main__':
    t1 = time.time()
    #ijsonMain()
    jsonMain()
    print("time spend:",time.time()-t1)