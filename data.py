import re
from sqlalchemy import create_engine
import psycopg2
import pandas as pd

filename = 'C://Users//H.Kim//Desktop//import_stock_data//SummaryFund.txt'


# Đọc từng dòng
    # Tách theo tab, xóa những kí tự trống 
    #   Nếu đủ 197 trường quăng vào chép vào dataframe 
    #   Ngược lại quăng chép vào file rồi quăng ra

# vấn đề là có những trường 199 cái chuỗi rỗng thì làm sao đây 
# tìm điểm khác nhau giữa những dòng trống và những dòng đầy đủ một cái mà đơn
# giản, tổng quát, và gọn nhẹ nhất

data = []

with open(filename,'r',encoding='utf-16') as file:
   line = file.readline()   
   while 1:
       # Tách theo tab, xóa kí tự trống 
       line = re.split('\t', line)
       if len(line) >= 197 and line[0]!='':
           line = line[0:196]
           data.append(line)
       line = file.readline()   
       if len(line) == 0:
           break   

# Create a DataFrame
columns = data[0]
data = data[1:]

# vấn đề là h xóa đi cái index của bảng là xong
dataFrame   = pd.DataFrame(data, columns=columns)
dataFrame.set_index('ID', inplace=True)


def get_dataframe(filename):
    data = []
    list_index_percent = [42, 49, 70, 72, 76, 82, 86, 90, 91, 93, 94, 96, 97, 101, 103, 104, 105, 109, 115, 117, 121, 122]
    list_index_big = [73, 134, 137, 138, 152, 153, 176, 54, 56,  10, 11, 12, 13, 37, 38, 39, 53, 68, 69, 71, 75, 77, 78, 81, 83, 114, 116, 120, 131, 133, 142, 143, 154, 155, 174, 175]
    
    # list_index_big = []
    with open(filename,'r',encoding='utf-16') as file:
        line = file.readline()   
        count = 0
        while 1:
            # Tách theo tab, xóa kí tự trống 
            line = re.split('\t', line)
            if len(line) >= 197 and line[0]!='':
                line = line[0:196]
                line = list(map(lambda x:x.replace('"', ''), line))
                map(lambda x:x[:-2], [line[i] for i in list_index_percent]) 
                
                for i in list_index_percent:
                    if count!=0:
                        line[i+1] = line[i+1][:-2]
                    # else:
                    #     line[i+1] =  line[i+1]+'_percent'   

                for i in list_index_big:
                    if count!=0:
                        line[i+1] = line[i+1].replace(',', '')
                        
               
                data.append(line)
                count+=1
            line = file.readline()   
            if len(line) == 0:
                break  

    # Create a DataFrame
    columns = data[0]
    columns[122] = columns[122].replace('"', '')
    columns[134] = columns[134].replace('"', '')
    columns[71] = '%/Capital_1'
    columns[73] = '%/Capital_2'
    columns[118] = '%/Gross/Revenue_1'
    columns[173] = '%/Gross/Revenue_2'
    columns[119] = '%Gross_Profit(t)_1'
    columns[120] = '%Gross_Profit(t)_2'
    columns[174] = '%GM_1'
    columns[178] = '%GM_2'
    columns[180] = 'BS_Missing_1'
    columns[184] = 'BS_Missing_2'
    columns[182] = 'IS_Missing_1'
    columns[186] = 'IS_Missing_2'

    a ='FP_[10%,_2009;_t,P/E=20.x_]'
    a = a.replace("[", "_")
    for i, column in enumerate(columns):
        columns[i] = columns[i].replace("(", "_")
        columns[i] = columns[i].replace(")", "")
        columns[i] = columns[i].replace("[", "_")
        columns[i] = columns[i].replace("]", "")
        columns[i] = columns[i].replace("-", "_sub_")
        columns[i] = columns[i].replace("+", "_plus_")
        columns[i] = columns[i].replace("/", "_divide_")
        columns[i] = columns[i].replace("=", "_equal_")
        columns[i] = columns[i].replace("%", "_percent_")
        columns[i] = columns[i].replace(".", "_dot_")
        columns[i] = columns[i].replace(",", "_comma_")
        columns[i] = columns[i].replace(";", "_semi_colon_")



    data = data[1:]
    
    
    # vấn đề là h xóa đi cái index của bảng là xong
    dataFrame   = pd.DataFrame(data, columns=columns)
    dataFrame.set_index('ID', inplace=True)
    # print(columns[54+1])
    # print(columns[84+1])
    
    return dataFrame

