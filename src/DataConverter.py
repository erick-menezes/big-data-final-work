import os
import re
import pandas as pd
import pymupdf
from typing import List, Dict
from pyspark.sql import SparkSession
from logging import Logger

class DataConverter:
    def __init__(
        self,
        LOGGER: Logger,
        spark_instance: SparkSession, 
        input_path: str,
        output_path: str
    ):
        self.spark_instance = spark_instance
        self.input_path = input_path
        self.output_path = output_path
        self.LOGGER= LOGGER

    def start(self) -> None:
        try:
            self.LOGGER.info('Listing all PDF files inside input folder')
            pdf_files = self.list_pdf_files(self.input_path)

            self.LOGGER.info('Reading data from PDF files')
            pdf_data = self.read_pdf_files(pdf_files)
            
            self.LOGGER.info('Converting and merging all data to a dataframe')
            df = self.extract_pdf_data_to_df(pdf_data)

            self.LOGGER.info('Saving dataframe to an excel file')
            self.save_to_excel(df)

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
    
    def read_pdf_files(self, pdf_files: List[str]) -> Dict:
        converted_pdfs = []

        for pdf_file in pdf_files:
            document = pymupdf.open(pdf_file)
            text = []

            for page in document:
                text.append(page.get_text())

            document.close()

            converted_pdfs.append({
                'filename': pdf_file.split('\\')[-1].split('.pdf')[0],
                'content': text
            })

        return converted_pdfs
    
    def extract_pdf_data_to_df(self, pdf_data: List[Dict]):
        df_data_model = {
            'company': [],
            # 'address': [],
            'purchase_date': [],
            'total_value': []
        }

        for data in pdf_data:
            company_matches = self.get_pattern_matches(r'Endereço:\s*(.*)', data['content'])
            # address_matches = self.get_pattern_matches(r'Endereço:\s*(.*)', data['content'])
            purchase_date_matches = self.get_pattern_matches(r'Data da compra:\s*(.*)', data['content'])
            total_value_matches = self.get_pattern_matches(r'Total:\s*(.*)', data['content'])

            df_data_model['company'] = df_data_model['company'] + company_matches
            # df_data_model['address'] = # df_data_model['address'] + address_matches
            df_data_model['purchase_date'] = df_data_model['purchase_date'] + purchase_date_matches
            df_data_model['total_value'] = df_data_model['total_value'] + total_value_matches

        return pd.DataFrame(df_data_model)
    
    def get_pattern_matches(self, regex: str, text: List[str]) -> List[str]:
        pattern = re.compile(regex)

        matches = []

        for sentence in text:
            match = pattern.search(sentence)
            if match:
                matches.append(match.group(1).strip())

        return matches

    def save_to_excel(self, dataframe: pd.DataFrame):
        file_name = 'company-info-report.xlsx'
        dataframe.to_excel(os.path.join(self.output_path, file_name), index=False)