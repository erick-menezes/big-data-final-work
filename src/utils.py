def format_brl(value):
    return f'R${value:,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.')