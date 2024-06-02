import pymupdf
import pandas as pd
import regex as re
from typing import List, Dict
from logging import Logger

class ClientService:
    def __init__(self, LOGGER: Logger):
        self.LOGGER= LOGGER

    def start_extraction(self, pdf_files: List[str]) -> pd.DataFrame:
        self.LOGGER.info('Getting PDF content.')
        pdf_data = self.get_content(pdf_files)

        self.LOGGER.info('Converting and merging all data to a dataframe')
        df = self.extract_and_convert_to_df(pdf_data)

        return df
    
    def get_content(self, pdf_files: List[str]) -> Dict:
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
    
    def extract_and_convert_to_df(self, pdf_data: List[Dict]) -> pd.DataFrame:
        df_clients_data_model = {}

        for data in pdf_data:
            client_info = self.get_pattern_matches(r'(?<=\nPESO LIQUIDO)(\n.+){24}', data['content'])

            if len(client_info) > 0:
                [
                    uf, 
                    district, 
                    city, 
                    hour_entrance_exit, 
                    emission_date,
                    phone,
                    entrance_exit_date,
                    address,
                    name,
                    cep,
                    document,
                    icms_base_calculation,
                    icms_value,
                    products_total_value,
                    shipment_value,
                    insurance_value,
                    note_total_value,
                    discount,
                    other_expenses,
                    ipi_total_value,
                    x,
                    shipment_for_account_type,
                    quantity,
                    y
                ] = client_info[0].split('\n')

                df_clients_data_model['uf'] = [uf] + df_clients_data_model.get('uf', [])
                df_clients_data_model['district'] = [district] + df_clients_data_model.get('district', [])
                df_clients_data_model['city'] = [city] + df_clients_data_model.get('city', [])
                df_clients_data_model['hour_entrance_exit'] = [hour_entrance_exit] + df_clients_data_model.get('hour_entrance_exit', [])
                df_clients_data_model['emission_date'] = [emission_date] + df_clients_data_model.get('emission_date', [])
                df_clients_data_model['phone'] = [phone] + df_clients_data_model.get('phone', [])
                df_clients_data_model['entrance_exit_date'] = [entrance_exit_date] + df_clients_data_model.get('entrance_exit_date', [])
                df_clients_data_model['address'] = [address] + df_clients_data_model.get('address', [])
                df_clients_data_model['name'] = [name] + df_clients_data_model.get('name', [])
                df_clients_data_model['cep'] = [cep] + df_clients_data_model.get('cep', [])
                df_clients_data_model['document'] = [document] + df_clients_data_model.get('document', [])
                df_clients_data_model['icms_base_calculation'] = [icms_base_calculation] + df_clients_data_model.get('icms_base_calculation', [])
                df_clients_data_model['icms_value'] = [icms_value] + df_clients_data_model.get('icms_value', [])
                df_clients_data_model['products_total_value'] = [products_total_value] + df_clients_data_model.get('products_total_value', [])
                df_clients_data_model['shipment_value'] = [shipment_value] + df_clients_data_model.get('shipment_value', [])
                df_clients_data_model['insurance_value'] = [insurance_value] + df_clients_data_model.get('insurance_value', [])
                df_clients_data_model['note_total_value'] = [note_total_value] + df_clients_data_model.get('note_total_value', [])
                df_clients_data_model['discount'] = [discount] + df_clients_data_model.get('discount', [])
                df_clients_data_model['other_expenses'] = [other_expenses] + df_clients_data_model.get('other_expenses', [])
                df_clients_data_model['ipi_total_value'] = [ipi_total_value] + df_clients_data_model.get('ipi_total_value', [])
                df_clients_data_model['x'] = [x] + df_clients_data_model.get('x', [])
                df_clients_data_model['shipment_for_account_type'] = [shipment_for_account_type] + df_clients_data_model.get('shipment_for_account_type', [])
                df_clients_data_model['quantity'] = [quantity] + df_clients_data_model.get('quantity', [])
                df_clients_data_model['y'] = [y] + df_clients_data_model.get('y', [])

        return pd.DataFrame(df_clients_data_model)
    
    def get_pattern_matches(self, regex: str, text: List[str]) -> List[str]:
        pattern = re.compile(regex)

        matches = []

        for sentence in text:
            match = pattern.search(sentence)
            if match:
                matches.append(match.group(0).strip())

        return matches