import os
import pandas as pd
from typing import List
from pyspark.sql import SparkSession
from logging import Logger

from services.ClientService import ClientService
from services.ProductService import ProductService

class DataConverter:
    def __init__(
        self,
        LOGGER: Logger,
        spark_instance: SparkSession,
        client_service: ClientService,
        product_service: ProductService,
        input_path: str,
        output_path: str
    ):
        self.spark_instance = spark_instance
        self.input_path = input_path
        self.output_path = output_path
        self.LOGGER= LOGGER
        self.client_service = client_service
        self.product_service = product_service

    def start(self) -> None:
        try:
            self.LOGGER.info('Listing all PDF files inside input folder')
            pdf_files = self.list_pdf_files(self.input_path)

            self.LOGGER.info('Starting client data extraction.')
            client_df = self.client_service.start_extraction(pdf_files)

            self.LOGGER.info('Saving client dataframe to an excel file')
            self.save_to_excel(client_df, 'company-info-report')

            self.LOGGER.info('Starting product data extraction.')
            product_df = self.product_service.start_extraction(pdf_files)

            self.LOGGER.info('Saving product dataframe to an excel file')
            self.save_to_excel(product_df, 'product-info-report')

            self.LOGGER.info('Stopping Spark instance')
        except Exception as error:
            self.LOGGER.error('Error while executing the script: ' + str(error))
        finally:
            self.spark_instance.stop()
    
    def list_pdf_files(self, input_path: str) -> List[str]:
        return [
            os.path.join(
                input_path, file_name
            ) for file_name in os.listdir(
                input_path
            ) if file_name.endswith(".pdf")
        ]
    
    

    def save_to_excel(self, dataframe: pd.DataFrame, filename: str):
        file_name = f'{filename}.xlsx'
        dataframe.to_excel(os.path.join(self.output_path, file_name), index=False)