'''File Converter application'''

'''importing necessary packages'''
import glob
import json
import re
import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import multiprocessing

'''loading environmet variables'''
load_dotenv()

'''getting column names'''
def get_column_names(schema, tb_name:str, sorting_key='column_position'):
    table= sorted(schema[tb_name],key =lambda  x : x[sorting_key])
    return [col['column_name'] for col in table]

'''reading csv files'''
def read_csv(file_path, schema, table_name):
    column_names = get_column_names(schema,table_name)
    return  pd.read_csv(file_path, names= column_names, chunksize=10000)

def truncate_table(db_conn, table_name):
    '''first truncate table'''
    engine = create_engine(db_conn)
    with engine.connect() as con:
        try:
            con.execute(text(f'TRUNCATE TABLE {table_name}'))
            con.commit()
        except Exception as e:
            print(f'Error truncating table "{table_name}": {e}')

def load_to_postgres(db_conn, chunk, table_name): 
    '''load  data to postgres'''

    # Fixing null values for products
    if table_name=='products':
        chunk.fillna({'product_description':''},inplace=True)

    '''If we put method=multi here it will be slow so try to use method =None (by default)'''
    chunk.to_sql(f'{table_name}', db_conn, if_exists='append', index=False)

    
  

'''migration logic'''
def migration(args):

    # getting arguments from the tuple
    data,src,table_name,db_conn= args

    files= glob.glob(f'{src}/{table_name}/part-*')

    if not len(files):
        raise NameError(f'No files found for {table_name}')
        
    for file in files:
        # print(f'processing {file}')
        print(f'processing {table_name}')
        df= read_csv(file_path=file,schema=data,table_name=table_name)
        

        '''passing data frames chunk to load in postgres'''
        try:
            truncate_table(db_conn,table_name)
            for idx, chunk in enumerate(df):
                print(f"Proceesing {idx+1} chunk of size {chunk.shape} of {table_name}")
                load_to_postgres(db_conn,chunk, table_name)

        except Exception as e:
           print(f'Error: {e} \nwhile processing: {table_name}')


'''main logic'''
def db_loader(ds_name=None):
    src_path= os.getenv('src_path')
    schema= json.load(open(f'{src_path}/schemas.json'))
    db_connn_uri= f"""postgresql://{os.getenv('db_user')}:{os.getenv('db_user_password')}@{os.getenv('db_host')}:{os.getenv('db_port')}/{os.getenv('db_name')}"""
    
    '''Taking 12 seconds w/o multiprocessing'''
    '''Taking 9 seconds with multiprocessing'''
    if not ds_name:
        ds_name=schema.keys()
    pprocessing= len(ds_name) if len(ds_name)<4 else 4
    pool= multiprocessing.Pool(pprocessing)
    pd_args=list()

    for table in ds_name:
        # migration(data=schema,src=src_path,table_name=table,db_conn=db_connn_uri)
        pd_args.append((schema,src_path,table,db_connn_uri))

    try:
        pool.map(migration, pd_args)

    except NameError as e:
        print(e)

if __name__=='__main__':
    if len(sys.argv)==2:
        table_names= json.loads(sys.argv[1])
        db_loader(table_names)
    else:
        db_loader()
