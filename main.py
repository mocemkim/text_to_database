from data import get_dataframe
import os 
import sys

# lấy data 
filename = os.path.join(str(os.getcwd()), 'data_test.txt') 
dataframe = get_dataframe(filename)
# print(dataframe)

# lấy kết nối
from connect import connect
conn = connect()

# chép vào database 
from dataframe2postgreSQL import write_frame
write_frame(dataframe, 'table_test', conn)