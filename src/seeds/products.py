import random
from typing import Dict
from faker import Faker

from utils import format_brl

product_descriptions = [
    'AR CONDICIONADO SPLIT INVERTER FRIO/QUENTE 24.000 BTUS COLOR ADPT ELECTROLUX',
    'NECTAR DE FRUTA TETRA PACK 200 ML',
    'MINI BOLO SEM RECHEIO MORANGO 40G',
    'MINI BOLO SEM RECHEIO 40G',
    'IOGURTE POLPA DE FRUTAS 90 G',
    'CAFÉ TORRADO E MOÍDO 500G',
    'LEITE CONDENSADO 395G',
    'PAPEL HIGIÊNICO NEVE 30M',
    'DESODORANTE AEROSSOL 150ML',
    'SHAMPOO ANTICASPA 200ML',
    'ESCOVA DENTAL ADULTO',
    'PÃO DE FORMA INTEGRAL 500G',
    'ARROZ PARBOILIZADO 1KG',
    'FEIJÃO CARIOCA 1KG',
    'AÇÚCAR REFINADO 1KG',
    'ÓLEO DE SOJA 900ML',
    'MARGARINA VEGETAL 250G',
    'ACHOCOLATADO EM PÓ 400G',
    'FARINHA DE TRIGO 1KG',
    'MACARRÃO INSTANTÂNEO 85G',
    'QUEIJO MUSSARELA 500G',
    'PRESUNTO COZIDO 200G',
    'PEITO DE FRANGO 1KG',
    'CARNE BOVINA MOÍDA 1KG',
    'BISCOITO RECHEADO 140G'
]

class ProductSeeder:
    faker: Faker = Faker()

    def generate_product(self) -> Dict:
        quantidade = round(random.uniform(1, 100), 2)
        valor_unitario = round(random.uniform(10, 1000), 2)
        valor_total = round(quantidade * valor_unitario, 2)
        base = round(random.uniform(1000, 5000), 2)
        aliquota_icms = round(random.uniform(0, 25), 2)
        aliquota_ipi = round(random.uniform(0, 10), 2)
        valor_icms = round(base * (aliquota_icms / 100), 2)
        valor_ipi = round(base * (aliquota_ipi / 100), 2)
    
        return {
            'CÓDIGO': f'{random.randint(1, 9999999999):010d}',
            'DESCRIÇÃO DOS PRODUTOS / SERVIÇOS': random.choice(product_descriptions),
            'NCM/SH': self.faker.ean(length=8),
            'CST/CSOSN': self.faker.random_element(elements=('0101', '0102', '0103', '0201', '0202', '0203')),
            'CFOP': self.faker.random_element(elements=('5101', '5102', '5405', '6101', '6102', '6403')),
            'UNID': self.faker.random_element(elements=('UN', 'KG', 'L', 'M2', 'M3')),
            'QUANT.': quantidade,
            'VALOR UNITÁRIO': format_brl(valor_unitario),
            'VALOR TOTAL': format_brl(valor_total),
            'BASE': format_brl(base),
            'VALOR - ICMS': format_brl(valor_icms),
            'VALOR - IPI': format_brl(valor_ipi),
            'ALIQUOTA - ICMS (%)': aliquota_icms,
            'ALIQUOTA - IPI (%)': aliquota_ipi,
        }