import io
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
    @staticmethod
    def extract_city_name(string):
        cleaned_string = re.sub(r'[^\w\s]', '', string)
        city_name = cleaned_string.strip()
        return city_name

    @staticmethod
    def extract_only_numbers(string):
        numbers = re.findall(r'\d+', string)
        return ''.join(numbers)

    @staticmethod
    def extract_floats_without_sign(string):
        string_without_dollar = string.replace('$', '')
        return float(string_without_dollar)


def load_df_from_s3(bucket, prefix, key=None, index_col=None, usecols=None, sep=','):
    ''' Read a csv from a s3 bucket & load into pandas dataframe'''
    s3_resource = boto3.resource('s3')
    try:
        logging.info(f"Loading {bucket}, {prefix}, {key}")
        bucket = s3_resource.Bucket(bucket)
        prefix_objs = bucket.objects.filter(Prefix=prefix)
        all_data = []
        for obj in prefix_objs:
            key = obj.key
            body = obj.get()['Body'].read()
            temp_df = pd.read_csv(io.BytesIO(body), encoding='utf8', index_col=index_col, sep=sep)
            all_data.append(temp_df)
        return pd.concat(all_data)
    except Exception as e:
        logger.exception(f"Error loading data from S3: {e}")
        raise


def data_cleaner(df):

    df['STORE_LOCATION'] = df['STORE_LOCATION'].map(ModifyColumns.extract_city_name)
    df['PRODUCT_ID'] = df['PRODUCT_ID'].map(ModifyColumns.extract_only_numbers)

    for to_clean in ['MRP', 'CP', 'DISCOUNT', 'SP']:
        df[to_clean] = df[to_clean].map(ModifyColumns.extract_only_numbers)

    return df


def upload_dataframe_into_rds(df):
    table_name = 'clean_transaction'
    sql_query = f"SELECT * FROM {table_name}"
    database_uri = GlobalVariables.database_uri
    try:
        engine = create_engine(database_uri)
        logger.info('Database connection successful')
    except Exception as e:
        logger.error('Database connection unsuccessful:', e)
        raise

    try:
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        logger.info(f'Dataframe uploaded into {table_name} successfully')
        uploaded_df = pd.read_sql(sql_query, engine)
        logger.info('\n' + uploaded_df.head().to_string(index=False))
    except Exception as e:
        logger.error('Error happened while uploading dataframe into database:', e)
        raise


def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    df = load_df_from_s3(bucket=bucket, prefix=key, sep=',')

    df_final = data_cleaner(df)

    upload_dataframe_into_rds(df_final)
