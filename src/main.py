import os

from DataConverter import DataConverter

from pyspark.sql import SparkSession
from logging import Logger

from seeds.clients import ClientSeeder
from seeds.products import ProductSeeder

from services.ClientService import ClientService
from services.ProductService import ProductService

CUSTOM_TEMP_DIR = 'tmp'
os.environ["SPARK_LOCAL_DIRS"] = CUSTOM_TEMP_DIR

if __name__ == '__main__':
    spark_instance = SparkSession.builder.appName(
        'Big Data - Final work'
    ).config(
        'spark.local.dir', CUSTOM_TEMP_DIR
    ).getOrCreate()
    
    spark_instance.sparkContext.setLogLevel("INFO")
    log4jLogger = spark_instance.sparkContext._jvm.org.apache.log4j
    LOGGER: Logger = log4jLogger.LogManager.getLogger(__name__)

    LOGGER.warn('Initiliazing DataConverter script')

    client_seeder = ClientSeeder()
    product_seeder = ProductSeeder()

    client_service = ClientService(LOGGER=LOGGER, client_seeder=client_seeder)
    product_service = ProductService(LOGGER=LOGGER, product_seeder=product_seeder)
    
    DataConverter(
        LOGGER=LOGGER,
        spark_instance=spark_instance, 
        client_service=client_service,
        product_service=product_service,
        input_path='input',
        output_path='output'
    ).start()