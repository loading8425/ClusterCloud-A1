from mpi4py import MPI
import json, ijson, os, math
import re
import threading
from utils import  process_large_json_file_v3
import queue
import time

def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    filename = "bigTwitter.json"

    # calculate chunk for each nodes
    file_size = os.path.getsize(filename)
    chunk_size = math.ceil(file_size / size)
    start = rank * chunk_size
    end = min(file_size, start + chunk_size)
    #--------------------First Node--------------------#
    if rank == 0:
        t1 = time.time()
        print("Program running on size of %d core"% (size))

        if size == 1:
            num_objects = 0
            location_tweets_count_dict = process_large_json_file_v3(filename,0)
            print(sorted(location_tweets_count_dict.items(), key=lambda x:x[1], reverse=True))
################################################ multi nodes #############################################
        else:
            location_tweets_count_dict = process_large_json_file_v3(filename,start,end)
            
            for r in range(1,size):
                data = comm.recv(source=r)
                for key, value in data.items():
                    if key not in data.keys():
                        location_tweets_count_dict[key] = value
                    else:
                        location_tweets_count_dict[key] += value
            print(sorted(location_tweets_count_dict.items(), key=lambda x:x[1], reverse=True))
        print(time.time()-t1)

    #--------------------Other Node--------------------#
    else:
        location_tweets_count_dict = process_large_json_file_v3(filename,start,end)
        
        #print("Core %d done, total number of objects = %d, results:"% (rank, num_objects), location_tweets_count_dict)
        # send back processed data back to root
        comm.send(location_tweets_count_dict, dest=0)

if __name__ == '__main__':
    main()