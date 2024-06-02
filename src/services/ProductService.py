import pymupdf
import pandas as pd
from typing import List, Dict
from logging import Logger
from functools import reduce

class ProductService:
    def __init__(self, LOGGER: Logger):
        self.LOGGER= LOGGER

    def start_extraction(self, pdf_files: List[str]) -> pd.DataFrame:
        self.LOGGER.info('Getting PDF content and converting to dataframe.')
        df = self.extract_and_convert_to_df(pdf_files)

        return df
    
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
        merged_data = merged_data.drop(columns=['Col0', 'Col15'])

        return merged_data