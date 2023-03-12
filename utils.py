import json

# 将大文件拆分成多个小文件
def split_json_file(input_file, output_prefix, chunk_size):
    # 计数器
    count = 0
    # 文件编号
    file_num = 1
    # 用于读取JSON数据的缓冲区
    data = []
    # 逐行读取JSON文件
    try:
        with open(input_file) as f:
            for line in f:
                # 将JSON字符串转换为Python对象
                print(line)
                obj = json.loads(line)
                # 将Python对象添加到缓冲区
                data.append(obj)
                count += 1
                # 如果缓冲区已经包含了chunk_size条记录，就将缓冲区的数据写入到小文件中
                if count == chunk_size:
                    output_file = f'{output_prefix}{file_num}.json'
                    with open(output_file, 'w') as out:
                        # 将Python对象转换为JSON字符串并写入到文件中
                        for obj in data:
                            out.write(json.dumps(obj) + '\n')
                    # 清空缓冲区
                    data = []
                    # 更新计数器和文件编号
                    count = 0
                    file_num += 1
        # 如果缓冲区中还有剩余的数据，就将它们写入到最后一个小文件中
        if count > 0:
            output_file = f'{output_prefix}{file_num}.json'
            with open(output_file, 'w') as out:
                for obj in data:
                    out.write(json.dumps(obj) + '\n')
    except json.decoder.JSONDecodeError as e:
        print(f"Error parsing JSON file {input_file}: {e}")
