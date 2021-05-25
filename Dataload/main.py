from Datalake import Loading_Raw_Data
from Dimension import Dim_Customer,Dim_Transaction_Type
from Facts import Fact_Transaction,Fact_Transaction_Line_Item
import traceback
from Utils import decryption



dl=Loading_Raw_Data.datalake_load()
dc=Dim_Customer.dim_customer_load()
dt=Dim_Transaction_Type.dim_transaction_type_load()
ft=Fact_Transaction.fact_transaction_load()
ftl=Fact_Transaction_Line_Item.fact_transaction_line_item_load()




def data_process():
    try:
        password=decryption.database_decryption()
        print ('Process to load datalake started')

        dl.raw_data_load('db','5432','database','username',password)


        print('Process finished to load datalake')


        print('Process to load dim_customer started')


        dc.data_load('db','5432','database','username',password)


        print('Process finished to load dim_customer')


        print('Process to load dim_transaction_type started')


        dt.data_load('db','5432','database','username',password)


        print('Process finished to load dim_transaction_type')


        print('Process to load fact_transaction started')


        ft.data_load('db','5432','database','username',password)


        print('Process finsihed to load fact_transaction')

        print('Process to load fact_transaction_line_item started')

        ftl.data_load('db','5432','database','username',password)


        print('Process finsihed to load fact_transaction_line_item')

    except:
        errMsg = traceback.format_exc()
        print('Not able to load  data to dim_customer because of following error ' + errMsg)


if __name__ == '__main__':
    data_process()

