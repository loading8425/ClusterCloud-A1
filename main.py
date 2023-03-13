# from mpi4py import MPI
import json, ijson
from utils import split_json_file
import re
# comm = MPI.COMM_WORLD
# rank = comm.Get_rank()

sal_filename = "sal.json"
filename = "twitter-data-small.json"

#load sal data
with open(sal_filename, 'r') as f:
    sal_data = json.load(f)
print(sal_data["west island"])
# load twitter data
with open(filename, 'rb') as f:
    objects = ijson.items(f, 'item')
    num_objects = 0
    for obj in objects:
        num_objects += 1
        if num_objects == 1000000:
            break
print(objects[0])

# if rank == 0:
#     data = {'a': 7, 'b': 3.14}
#     comm.send(data, dest=1, tag=11)
# elif rank == 1:
#     data = comm.recv(source=0, tag=11)
#     print(data)
# else:
#     print(rank,"idle")