import pymupdf
import pandas as pd
from typing import List, Dict
from logging import Logger

from seeds.products import ProductSeeder

class ProductService:
    def __init__(self, LOGGER: Logger, product_seeder: ProductSeeder):
        self.LOGGER = LOGGER
        self.product_seeder = product_seeder

    def start_extraction(self, pdf_files: List[str]) -> pd.DataFrame:
        self.LOGGER.info('Getting PDF content and converting to dataframe.')
        df = self.extract_and_convert_to_df(pdf_files)

        self.LOGGER.info('Populating dataframe with extra data')
        df_data = self.populate_dataframe(df)

        return df_data
    
    def extract_and_convert_to_df(self, pdf_files: List[str]) -> pd.DataFrame:
        dataframes = []

        for pdf_file in pdf_files:
            document = pymupdf.open(pdf_file)
            page = document[0]
            tabs = page.find_tables(pymupdf.Rect(0, 471.0048828215, 600, 671.8017578125))

            for i, tab in enumerate(tabs):
                df = tab.to_pandas()
                dataframes.append(df)

            document.close()

        # Merging all dataframes of each file into one
        merged_data = pd.concat(dataframes, ignore_index=True)

        # Removing unused columns
        merged_data = merged_data.drop(
            columns=['Col0', 'Col15']
        ).rename(
            columns={ 
                'Col12': 'VALOR - IPI', 
                'Col14': 'ALIQUOTA - IPI (%)',
                'VALOR': 'VALOR - ICMS',
                'ALIQUOTA': 'ALIQUOTA - ICMS (%)',
                'CST/CSOS\nN': 'CST/CSOSN',
                'VALOR\nUNITÁRIO': 'VALOR UNITÁRIO'
            }
        ).dropna(subset=['CÓDIGO'])

        return merged_data
    
    def populate_dataframe(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        new_products = [self.product_seeder.generate_product() for _ in range(25)]

        return pd.concat(
            [dataframe, pd.DataFrame(new_products)], 
            ignore_index=True
        )
