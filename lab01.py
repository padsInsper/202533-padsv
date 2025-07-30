import pandas as pd

olist = pd.read_parquet(
    "https://github.com/padsInsper/202533-padsv/releases/download/dados/olist_items.parquet"
)

pagamento_counts = olist['types'].value_counts().reset_index()
pagamento_counts.columns = ['Forma de pagamento', 'Quantidade']
pagamento_filtro = pagamento_counts[pagamento_counts['Quantidade'] > 100]

pagamento_filtro['Quantidade (milhares)'] = round(pagamento_filtro['Quantidade'] / 1000, 2)

pagamento_filtro


