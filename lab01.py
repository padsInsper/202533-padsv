import pandas as pd

olist = pd.read_parquet(
    "https://github.com/padsInsper/202533-padsv/releases/download/dados/olist_items.parquet"
)

pagamento_counts = olist['types'].value_counts().reset_index()
pagamento_counts.columns = ['Forma de pagamento', 'Quantidade']
pagamento_filtro = pagamento_counts[pagamento_counts['Quantidade'] > 100]

pagamento_filtro['Quantidade (milhares)'] = round(pagamento_filtro['Quantidade'] / 1000, 2)

pagamento_filtro['n'] = pagamento_filtro['Quantidade (milhares)']

from plotnine import *

(
    ggplot(pagamento_filtro, aes(y = 'Quantidade (milhares)', x = 'Forma de pagamento'))
    + geom_col()
    + coord_flip()
    + geom_label(aes(label = 'round(n, 2)', y = 'n / 2'))
)



