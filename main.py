from data import get_dataframe

# lấy data 
filename = 'C://Users//H.Kim//Desktop//import_stock_data//SummaryFund.txt'
dataframe = get_dataframe(filename)

print('________________a_______________________')

# lấy kết nối
from connect import connect
conn = connect()


# chép vào database 
from dataframe2postgreSQL import write_frame
write_frame(dataframe, 'SumaryFundStock', conn)