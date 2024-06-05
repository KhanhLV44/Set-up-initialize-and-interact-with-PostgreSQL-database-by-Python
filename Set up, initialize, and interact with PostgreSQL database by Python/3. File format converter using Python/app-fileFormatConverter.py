import glob
import os
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
    file_name = file_path_list[-1]
    columns = get_column_names(schemas, table_name)
    df = pd.read_csv(file, names=columns)
    return df

def to_json(df, tgt_base_dir, table_name, file_name):
    json_file_path = f'{tgt_base_dir}/{table_name}/{file_name}'
    os.makedirs(f'{tgt_base_dir}/{table_name}', exist_ok=True)
    df.to_json(
        json_file_path,
        orient='records',
        lines=True
    )

def file_converter(src_base_dir, tgt_base_dir, table_name):
    schemas = json.load(open(f'{src_base_dir}/schemas.json'))
    files = glob.glob(f'{src_base_dir}/{table_name}/*')

    for file in files:
        df = read_csv(file, schemas)
        patterns = r'/|\\'
        file_name = re.split(patterns, file)[-1]
        to_json(df, tgt_base_dir, table_name, file_name)

def process_files(table_names=None):
    src_base_dir = 'D:/DE projects/project/data/retail_db'
    tgt_base_dir = 'D:/DE projects/project/data/retail_db_json'
    schemas = json.load(open(f'{src_base_dir}/schemas.json'))

    if not table_names:
        table_names = schemas.keys()
    
    for table_name in table_names:
        print(f'Processing {table_name}')
        file_converter(src_base_dir, tgt_base_dir, table_name)

if __name__ == '__main__':
    process_files()

# THE END! (54 lines total)