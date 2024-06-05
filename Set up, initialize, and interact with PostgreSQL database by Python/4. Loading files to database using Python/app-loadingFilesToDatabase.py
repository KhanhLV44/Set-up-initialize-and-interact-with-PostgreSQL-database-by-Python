import glob
import json
import re
import pandas as pd

def get_column_names(schemas, table_name, sorting_key='column_position'):
    column_details = schemas[table_name]
    columns = sorted(column_details, key=lambda col: col[sorting_key])
    return [col['column_name'] for col in columns]

def read_csv(file, schemas):
    patterns = r'/|\\'
    file_path_list = re.split(patterns, file)
    table_name = file_path_list[-2]
    columns = get_column_names(schemas, table_name)
    df = pd.read_csv(file, names=columns, chunksize=10000)
    return df

def to_sql(df, conn, table_name):
    df.to_sql(table_name,
            conn,
            if_exists='append',
            index=False)

def db_loader(src_base_dir, conn, table_name):
    schemas = json.load(open(f'{src_base_dir}/schemas.json'))
    files = glob.glob(f'{src_base_dir}/{table_name}/part-*')
    
    if len(files) == 0:
        raise NameError(f'No files found for {table_name}')
    
    for file in files:
        df = read_csv(file, schemas)
        for idx, df in enumerate(df):
            print(f'Populating chunk {idx} of {table_name}')
            to_sql(df, conn, table_name)

def process_files(table_names=None):
    src_base_dir = 'D:/DE projects/project/data/retail_db'
    db_host = 'localhost'
    db_port = 5432
    db_name = 'retail_db'
    db_user = 'khanh_user'
    db_pass = '66668888'
    conn = f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}'
    schemas = json.load(open(f'{src_base_dir}/schemas.json'))
    
    if not table_names:
        table_names = schemas.keys()
    
    for table_name in table_names:
        try:
            print(f'Processing {table_name}')
            db_loader(src_base_dir, conn, table_name)
        except NameError as ne:
            print(ne)
            pass
        except Exception as e:
            print(e)
            pass
        finally:
            print(f'-----------------------> Finish processing {table_name}')

if __name__ == '__main__':
    process_files()

# THE END! (66 lines total)