import json
from ctypes import sizeof

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
            start = file.tell() - len(line) - 4
    return start

def read_one_json_obj(file_path,start)->dict:
    start = find_next_start_json(file_path, start)
    if start == -1:
        return None, -1
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

def read_json_file(file_path, start, num_obj):
    dic_list = []
    end = start
    while len(dic_list)<num_obj:
        objs,end = read_one_json_obj(file_path, end)
        if objs==None:
            break
        dic_list.append(objs)
    #print(dic_list)
    #print(len(dic_list))
    return dic_list, end

if __name__ == '__main__':
    file_path = 'test.json'
    start = 0
    dic_list, end = read_json_file(file_path,start, 3)
    dic_list2,_ = read_json_file(file_path,end, 99)
    #print(dic_list)
    print(len(dic_list))
    print(len(dic_list2))
