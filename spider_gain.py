a = ['1.bin', 'x.bin','12.bin', '2.bin','s1.bin']

def sort_id(x):
    ids = []
    str_ids = []
    item = {}
    for index,value in enumerate(x):
        file_name = value.split('.')[0]
        item[value] = index
        try:
            int(file_name)
            ids.append(int(file_name))
        except:
            str_ids.append(value)
    int_ids = sorted(ids,key = lambda i:i)
    int_files_indexs = [item[str(i)+'.bin'] for i in int_ids]
    int_files = [x[i] for i in int_files_indexs]
    str_fields = sorted(str_ids,key=lambda key:key.split('.')[0])
    int_files.extend(str_fields)
    return int_files

int_files =sort_id(a)
print(int_files)





