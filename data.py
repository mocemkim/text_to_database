import re
from sqlalchemy import create_engine
import psycopg2
import pandas as pd
import sys
import os


# readline để dọc từng dòng
# Ở mỗi dòng tách ra =, regex bắt số khoảng trắng không cố 
# định ấy

def get_dataframe(filename):
    mul_space = '\s+'
    data = []
    with open(filename, 'r', encoding='utf-8') as file:
        line = file.readline()
        while 1:
            line = re.split(mul_space, line)
            data.append(line)
            line = file.readline()
            if len(line)==0:
                break
    
    # columns bỏ đi kí tự rỗng đầu tiên và cuối cùng
    columns = data[0][1:-1]
    # dòng ở giữa bỏ đi dòng index đầu tiên, và rỗng ở cuối
    mid_data = [row[1:-1]for row in data[1:-1]]
    # dòng cuối cùng chỉ cần bỏ cái index đầu tiên
    last_row = data[-1][1:]

    mid_data.append(last_row)
    data = mid_data
    dataFrame = pd.DataFrame(data, columns = columns)
    print(dataFrame)


    
    return dataFrame
