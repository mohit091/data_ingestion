import pandas as pd
from datetime import datetime
from  Utils import connection
import traceback


class datalake_load():

    def raw_data_load(self,host,port,dbname,user,password):
        try:
            host=host
            port=port
            dbname=dbname
            user=user
            password=password
            conn_details=connection.database_connection.connection(host,port,dbname,user,password)
            conn=conn_details[0]
            cur=conn_details[1]
            df = pd.read_csv('Source_Data/source_data.csv', sep='\t')
            print(df)

            current_ts = datetime.now()
            now = current_ts.strftime("%Y-%m-%d %H:%M:%S")
            df["ETL_LOAD_TIME"] = now
            print(df)
            json = df.to_json('raw_data.json', orient='records', lines=True)

            cur.execute('delete from DATALAKE.TRANSACTION_RAW_DATA src where src.etl_load_time::date=current_date')
            copy_sql = """
                       COPY DATALAKE.TRANSACTION_RAW_DATA (TRANSACTION_DATA) FROM stdin csv quote e'\x01' delimiter e'\x02'
                       """
            with open('raw_data.json', 'r') as f:
                cur.copy_expert(sql=copy_sql, file=f)
                cur.close()
                conn.close()
            print ('raw data loaded successfully')
        except:
            errMsg = traceback.format_exc()
            print('Not able to load  data to datalake because of following error ' + errMsg)

