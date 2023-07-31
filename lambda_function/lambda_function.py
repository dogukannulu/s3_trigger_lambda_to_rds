import logging
import pandas as pd
import boto3
import re
from datetime import datetime
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class GlobalVariables:
    bucket_name = 'csv-to-s3-project-dogukan-ulu'
    bucket_key = 'dirty_store_transactions/dirty_store_transactions.csv'
    database_name = '<database_name>'
    database_username = '<user_name>'
    database_password = '<password>'
    database_endpoint = '<database_endpoint>'
    database_port = 3306
    s3_client = boto3.client('s3')
    database_uri = f"mysql+pymysql://{database_username}:{database_password}@{database_endpoint}:{database_port}/{database_name}"


class ModifyColumns:
    def extract_city_name(self, string):
        cleaned_string = re.sub(r'[^\w\s]', '', string)
        city_name = cleaned_string.strip()
        return city_name

    def extract_only_numbers(self, string):
        numbers = re.findall(r'\d+', string)
        return ''.join(numbers)

    def extract_floats_without_sign(self, string):
        string_without_dollar = string.replace('$', '')
        return float(string_without_dollar)


def upload_dataframe_into_rds(df):
    table_name = 'clean_transaction'
    sql_query = f"SELECT * FROM {table_name}"
    database_uri=GlobalVariables.database_uri
    try:
        engine = create_engine(database_uri)
        logger.info('Database connection successful')
    except Exception as e:
        logger.warning('Database connection unsuccessful:', e)

    try:
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        logger.info(f'Dataframe uploaded into {table_name} successfully')
        uploaded_df = pd.read_sql(sql_query, engine)
        print(uploaded_df.head())
    except Exception as e:
        logger.error('Error happened while uploading dataframe into database:', e)


def lambda_handler(event, context):
    now = datetime.now()
    now_ts = now.strftime("%Y%m%d%H%M%S")
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket="csv-to-s3-project-dogukan-ulu", Key="dirty_store_transactions/dirty_store_transactions.csv")
    df = pd.read_csv(obj['Body'])

    df['STORE_LOCATION'] = df['STORE_LOCATION'].map(lambda x: ModifyColumns.extract_city_name)


    df['PRODUCT_ID'] = df['PRODUCT_ID'].map(lambda x: ModifyColumns.extract_only_numbers)

    for to_clean in ['MRP', 'CP', 'DISCOUNT', 'SP']:
        df[to_clean] = df[to_clean].map(lambda x: ModifyColumns.extract_only_numbers)

    upload_dataframe_into_rds(df)