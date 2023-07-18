# import duckdb

# con = duckdb.connect('datbase')


# sql = """
# CREATE OR REPLACE TABLE test_data AS
# SELECT *
# FROM read_csv_auto('data\SorensonworkfirmFIVEdata.csv')
# """
# con.sql(sql)
        
    
# dat = con.sql("SELECT * FROM test_data LIMIT 5").fetchall()
# for row in dat[:10]:
#     print(row)
    
import os
import sys
# n= os.environ.copy()
# print (n)
current_dir = os.path.dirname(os.path.abspath(__file__))
print(current_dir)
print(os.getcwd())