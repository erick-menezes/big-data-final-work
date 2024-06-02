import random
from typing import Dict
from faker import Faker
from datetime import datetime

from utils import format_brl

class ClientSeeder:
    faker: Faker = Faker('pt_BR')

    def generate_client(self) -> Dict:
        icms_base_calculation = round(random.uniform(0, 5000), 2)
        icms_value = round(random.uniform(0, 500), 2)
        products_total_value = round(random.uniform(1000, 10000), 2)
        shipment_value = round(random.uniform(0, 500), 2)
        insurance_value = round(random.uniform(0, 300), 2)
        note_total_value = round(random.uniform(10500, 51000), 2)
        discount = round(random.uniform(0, 500), 2)
        other_expenses = round(random.uniform(0, 100), 2)
        ipi_total_value = round(random.uniform(0, 500), 2)
        x = round(random.uniform(0, 10), 2)
        y = round(random.uniform(0, 10), 2)

        return {
            'uf': self.faker.state_abbr(),
            'district': self.faker.bairro(),
            'city': self.faker.city(),
            'hour_entrance_exit': self.faker.time(),
            'emission_date': self.faker.date_between_dates(
                datetime(2023, 6, 2),
                datetime.now()
            ).strftime('%d/%m/%y'),
            'phone': self.faker.phone_number(),
            'entrance_exit_date': self.faker.date(),
            'address': self.faker.address(),
            'name': self.faker.name(),
            'cep': self.faker.postcode(),
            'document': self.faker.cnpj(),
            'icms_base_calculation': format_brl(icms_base_calculation),
            'icms_value': format_brl(icms_value),
            'products_total_value': format_brl(products_total_value),
            'shipment_value': format_brl(shipment_value),
            'insurance_value': format_brl(insurance_value),
            'note_total_value': format_brl(note_total_value),
            'discount': format_brl(discount),
            'other_expenses': format_brl(other_expenses),
            'ipi_total_value': format_brl(ipi_total_value),
            'x': format_brl(x),
            'shipment_for_account_type': random.choice(['0-EMITENTE']),
            'quantity': random.randint(1, 100),
            'y': format_brl(y),
        }