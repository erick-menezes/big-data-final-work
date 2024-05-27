import os

from DataConverter import DataConverter

from pyspark.sql import SparkSession
from logging import Logger

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
    # LOGGER.setLevel(log4jLogger.Level.INFO)

    LOGGER.warn('Initiliazing DataConverter script')
    
    DataConverter(
        LOGGER=LOGGER,
        spark_instance=spark_instance, 
        input_path='input',
        output_path='output'
    ).start()