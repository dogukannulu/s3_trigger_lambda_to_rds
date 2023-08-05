import io
import os
import re
import pandas as pd
import boto3
import logging
import pymysql
from sqlalchemy import create_engine
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class GlobalVariables:
    database_name = os.getenv('database_name')
    database_username = os.getenv('database_username')
    database_password = os.getenv('database_password')
    database_endpoint = os.getenv('database_endpoint')
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


def load_df_from_s3(bucket_name, key):
    """
    Read a CSV from a S3 bucket & load into pandas dataframe
    """
    s3 = GlobalVariables.s3_client
    logger.info("Starting S3 object retrieval process...")
    try:
        get_response = s3.get_object(Bucket=bucket_name, Key=key)
        logger.info("Object retrieved from S3 bucket successfully")
    except ClientError as e:
        logger.error(f"S3 object cannot be retrieved: {e}")
    
    file_content = get_response['Body'].read()
    df = pd.read_csv(io.BytesIO(file_content)) # necessary transformation from S3 to pandas

    return df


def data_cleaner(df):
    """
    Clean the data and return the cleaned dataframe
    """
    df['STORE_LOCATION'] = df['STORE_LOCATION'].map(ModifyColumns.extract_city_name)
    df['PRODUCT_ID'] = df['PRODUCT_ID'].map(ModifyColumns.extract_only_numbers)

    for to_clean in ['MRP', 'CP', 'DISCOUNT', 'SP']:
        df[to_clean] = df[to_clean].map(ModifyColumns.extract_only_numbers)

    return df


def upload_dataframe_into_rds(df):
    """
    Connect to RDS, upload the dataframe into the database
    """
    table_name = 'clean_transaction'
    sql_query = f"SELECT * FROM {table_name}"
    database_uri = GlobalVariables.database_uri
    logger.info("Starting the RDS Connection process...")
    try:
        engine = create_engine(database_uri)
        logger.info('RDS Database connection successful')
    except Exception as e:
        logger.error(f'RDS Database connection unsuccessful: {e}')
        raise
    
    logger.info("Starting to upload the dataframe into RDS database")
    try:
        df.to_sql(table_name, con=engine, if_exists='append', index=False)
        logger.info(f'Dataframe uploaded into {GlobalVariables.database_name}.{table_name} successfully')
        
        uploaded_df = pd.read_sql(sql_query, engine)
        logger.info('\n' + uploaded_df.head(5).to_string(index=False))
    except Exception as e:
        logger.error(f'Error happened while uploading dataframe into database: {e}')
        raise


def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    logger.info(f"S3 bucket is obtained from the event: {bucket}")
    logger.info(f"Object key is obtained from the event: {key}")

    df = load_df_from_s3(bucket_name=bucket, key=key)
    df_final = data_cleaner(df)
    upload_dataframe_into_rds(df_final)

    logger.info("Whole process completed.")
