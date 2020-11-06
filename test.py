import numpy as np
import random
import pandas as pd

'column_1_int column_2_float column_3_char'
# thêm 1 chút ý tưởng là có thể thêm cái datetime vào vì nó
# khá là thực tế

# cho chạy i in 1 ->100
# thêm vào column_i + random(int, float, char)
# cho chạy i in 1 ->100
#   Đọc từ [i:] nếu int thì random int, tương tự với float   
columns = []
for i in range(1, 11, 1 ):
    'a: {}, b: {}, c: {}'.format(1, 2, 3)
    val = 'column_{}_{}'.format(i, random.choice(['int', 'float', 'char']))
    columns.append(val)

data = []
for i in range(2):
    row = []
    for col in columns:
        datatype = col.split('_')[-1]
        if datatype=='int':
            tem = random.choice(range(1000))
            
        elif datatype == 'float':
            tem = random.uniform(10, 20)
            rand = round(random.choice(range(6)))
            tem = round(tem, rand)
        else:
            tem = 'abcd1234'
        row.append(tem)  
    data.append(row)

df = pd.DataFrame(data, columns=columns )
print(print(df.columns.tolist()))
print(df)

with open("data_test.txt", "w") as text_file:
    text_file.write(df.to_string())

     

