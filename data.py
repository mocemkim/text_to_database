import re
from sqlalchemy import create_engine
import psycopg2
import pandas as pd
import sys
import os


filename = os.path.join(str(os.getcwd()), 'data_test.txt') 
print(filename)


# Đọc từng dòng
    # Tách theo tab, xóa những kí tự trống 
    #   Nếu đủ 197 trường quăng vào chép vào dataframe 
    #   Ngược lại quăng chép vào file rồi quăng ra

# vấn đề là có những trường 199 cái chuỗi rỗng thì làm sao đây 
# tìm điểm khác nhau giữa những dòng trống và những dòng đầy đủ một cái mà đơn
# giản, tổng quát, và gọn nhẹ nhất

def get_dataframe(filename):
    data = []
    mul_space = '/\s/'
    with open(filename,'r',encoding='utf-8') as file:
        line = file.readline()
        while 1:
            # Tách theo tab, xóa kí tự trống
            line = re.split('\s+', line)
            data.append(line)
            line = file.readline()   
            if len(line) == 0:
                break 
    # columns[0], columns[-1] = ' '
    columns = data[0][1:-1] 
    tem_data = [data[1:-1][0][1:-1]]
    last_row = data[-1][1:]
    tem_data.append(last_row)
    data = tem_data

    dataFrame  = pd.DataFrame(data, columns = columns)
    return dataFrame
