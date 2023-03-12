# from mpi4py import MPI
import json
from utils import split_json_file

# comm = MPI.COMM_WORLD
# rank = comm.Get_rank()

#split_json_file('twitter-data-small.json','small',1000)

with open('twitter-data-small.json','r', encoding="utf-8") as f:
    for line in f.readlines():
        try:
            # 解析每行 JSON 对象
            print(line)
            obj = json.loads(line)
            # 在这里对 obj 进行处理
            print(obj)
            break
        except Exception as e:
            # 如果解析 JSON 对象时出现错误，则在这里处理异常
            #print(f'Error: {e}')
            pass
    


# if rank == 0:
#     data = {'a': 7, 'b': 3.14}
#     comm.send(data, dest=1, tag=11)
# elif rank == 1:
#     data = comm.recv(source=0, tag=11)
#     print(data)
# else:
#     print(rank,"idle")