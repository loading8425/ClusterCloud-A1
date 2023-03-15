from mpi4py import MPI
import json, ijson
import re
import threading
from utils import compare_city_name, process_data
import queue
import time

def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    filename = "smallTwitter.json"

    location_tweets_count_dict = {'1gsyd':0,'2gmel':0,'3gbri':0,'4gade':0,'5gper':0,'6ghob':0}

    #--------------------First Node--------------------#
    if rank == 0:
        print("Program running on size of %d core"% (size))
        # load twitter data
        with open(filename, 'rb') as f:
            objects = ijson.items(f, 'item')
            # in case only one node ############
            if size == 1:
                num_objects = 0
                for obj in objects:
                    num_objects += 1
                    place = obj["includes"]['places'][0]['full_name']
                    gcc = compare_city_name(place)
                    if gcc in location_tweets_count_dict.keys():
                        location_tweets_count_dict[gcc]+=1
                
                print(sorted(location_tweets_count_dict.items(), key=lambda x:x[1], reverse=True))
            else:
            # multi nodes ############
                num_objects = 0
                requests = []
                num_objects_core0 = 0
                # using thread to process tasks for root node
                # obj_queue = queue.Queue()
                # worker = threading.Thread(target=process_message_queue, args=(obj_queue,))
                # worker.start()

                # distribute task to other node
                for obj in objects:
                    dest = num_objects%size
                    num_objects += 1

                    # distribute task to root node thread
                    if dest==0:
                        num_objects_core0+=1
                        #obj_queue.put(obj)
                        continue
                    requests.append(comm.isend(obj, dest=dest))
                
                # end of file
                #obj_queue.put(-1)
                for r in range(1,size):
                    requests.append(comm.isend(False, dest=r))
                #worker.join()

                # wait for other cores' data
                #results = obj_queue.get()
                results = {}
                print("thread:",results)
                for r in range(1,size):
                    request = comm.irecv(source=r)
                    result = request.wait()
                    for key, value in result.items():
                        if key not in results.keys():
                            results[key] = value
                        else:
                            results[key] += value

                print(sorted(results.items(), key=lambda x:x[1], reverse=True))
                MPI.Request.waitall(requests)

    #--------------------Other Node--------------------#
    else:
        #message_queue = queue.Queue()
        num_objects = 0
        while True:
            request = comm.irecv(source=0)
            data = request.wait()
            if not data:
                break
            num_objects+=1
            place = data["includes"]['places'][0]['full_name']
            gcc = compare_city_name(place)
            if gcc in location_tweets_count_dict.keys():
                location_tweets_count_dict[gcc]+=1
        
        #print("Core %d done, total number of objects = %d, results:"% (rank, num_objects), location_tweets_count_dict)
        # send back processed data back to root
        comm.isend(location_tweets_count_dict, dest=0).wait()

    comm.Barrier()

def process_message_queue(mq:queue.Queue):
    location_tweets_count_dict = {'1gsyd':0,'2gmel':0,'3gbri':0,'4gade':0,'5gper':0,'6ghob':0}
    while True:
        if mq.qsize() == 0:
            continue
        else:
            data = mq.get()
            if data==-1:
                mq.put(location_tweets_count_dict)
                break
            place = data["includes"]['places'][0]['full_name']
            gcc = compare_city_name(place)
            if gcc in location_tweets_count_dict.keys():
                location_tweets_count_dict[gcc]+=1

if __name__ == '__main__':
    t1 = time.time()
    main()
    print(time.time()-t1)