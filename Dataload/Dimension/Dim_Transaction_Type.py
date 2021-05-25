from Utils import connection
import traceback


class dim_transaction_type_load():
    '''Class to perform all the ingestion activities for dim_transaction_type'''

    def data_load(self, host, port, dbname, user, password):
        '''function to load data to dim transaction type table'''
        try:
            host = host
            port = port
            dbname = dbname
            user = user
            password = password
            conn_details = connection.database_connection.connection(host, port, dbname, user, password)
            conn = conn_details[0]
            cur = conn_details[1]
            dim_transaction_type_insert_sql = "insert into TRANSACTIONS.DIM_TRANSACTION_TYPE (TRANSACTION_TYPE_NAME) select src.TRANSACTION_TYPE_NAME from (select distinct transaction_data->>'Type' as TRANSACTION_TYPE_NAME from  datalake.transaction_raw_data where transaction_data->>'Type' is not null and etl_load_time::date=current_date) src left join TRANSACTIONS.DIM_TRANSACTION_TYPE trg on src.TRANSACTION_TYPE_NAME=trg.TRANSACTION_TYPE_NAME where trg.TRANSACTION_TYPE_NAME is null;"
            cur.execute(dim_transaction_type_insert_sql)
            print('DIM_TRANSACTION_TYPE INSERTED SUCCESSFULLY')
            cur.close()
            conn.close()
            print('DIM_TRANSACTION_TYPE LOADED SUCCESSFULLY')

        except:
            errMsg = traceback.format_exc()
            print('Not able to load  data to dim_transaction_type because of following error ' + errMsg)
