import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd

print('Opening..')
df = pd.read_csv('C:/Users/somir/Desktop/archive/Output.csv', sep=',', header=0, index_col=False)
df.reset_index(drop=True, inplace=True)
print(df.head(10))
print('Opening snowflake..')

cnn = snowflake.connector.connect(
    user='mrunal',
    password='Welcome24@',
    account='fza88194.us-east-1',
    warehouse='project_warehouse_assignment',
    database='project_database_assignment',
    schema='project_schema_assignment'
    )
success, nchunks, nrows, _ = write_pandas(cnn, df, 'project_assignment', quote_identifiers=False)
print(str(success) + ', '+ str(nchunks) + ', ' + str(nrows))
cnn.close()
print('done..')
