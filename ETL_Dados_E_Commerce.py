#%% import das libs necessárias
# !pip install kaggle
import kaggle
import pandas as pd
import numpy as np 
import sqlite3
import pyarrow
from os import listdir, getcwd, remove

#%%
# listando os datasets disponiveis para download
#!kaggle datasets files -d olistbr/brazilian-ecommerce

#faz o download e o unzip de todos os arquivos na pasta do projeto para ter sempre os arquivos mais atualizados
#!kaggle datasets download olistbr/brazilian-ecommerce --unzip

# setando pandas pra mostrar todas as colunas dos dfs
pd.set_option('display.max_columns', None)

# convertendo os arquivos para parquet e removendo os csv
arquivos_csv = [arquivo for arquivo in listdir() if arquivo.endswith('.csv')]

for arquivo in arquivos_csv:
    df = pd.read_csv(arquivo) # carrega o csv em um df
    df.to_parquet(arquivo.removesuffix('.csv') + '.parquet') # exporta para um arquivo .parquet
    remove(arquivo) # remove os csvs da pasta

#%%
# --------------- df_products ------------------------
df_products = pd.read_parquet('olist_products_dataset.parquet', engine='pyarrow')

# selecionando apenas id e nome do produto
df_products = df_products[['product_id', 'product_category_name']].fillna({'product_category_name': 'geral'})

#%%
# --------------- df_orders ------------------------
df_orders = pd.read_parquet('olist_orders_dataset.parquet', engine='pyarrow')

# checando as qtd de linhas em que pelo menos uma das colunas esteja null
qtd_linhas_null = df_orders.query("""
                                    order_delivered_customer_date.isnull() or \
                                    order_delivered_carrier_date.isnull() or \
                                    order_approved_at.isnull()""") \
                           .shape[0]

# qtd total de linhas do df
total_linhas_df_orders = df_orders.shape[0]

# se o total de nulls for menor que 30% do total de linhas do df_orders, droppa as linhas
if qtd_linhas_null / total_linhas_df_orders < .3:
    df_orders = df_orders.dropna(subset=['order_delivered_customer_date', 
                                         'order_delivered_carrier_date', 
                                         'order_approved_at'],
                                 how='any')
    print(f'Qtd de nulls abaixo de 0.3. Linhas droppadas com sucesso!')

else:
    print(f'Quantidade de nulls viola o limite permitido.')

# convertendo as colunas tempo para data e hora
cols = df_orders.columns[3:] # lista de colunas a ser convertida
df_orders[cols] = df_orders[cols].apply(pd.to_datetime)

#%%
# # --------------- df_order_items ------------------------
df_order_items = pd.read_parquet('olist_order_items_dataset.parquet', engine='pyarrow')

# somando price + freight_value
df_order_items['total_price'] = df_order_items['price'] + df_order_items['freight_value']

# droppando as colunas price e freight_value e agrupando os dados
df_order_items = df_order_items.drop(columns=['price', 'freight_value'])\
                               .groupby(by=['order_id', 'product_id', 'seller_id', 'shipping_limit_date'])['total_price'].sum()\
                               .reset_index()

#%%
# df_order_payments
df_order_payments = pd.read_parquet('olist_order_payments_dataset.parquet', engine='pyarrow')

# agregando order_id e payment type pela qtd de parcelas e valor de pagamentos
df_order_payments = df_order_payments.groupby(by=['order_id', 'payment_type']).agg({'payment_installments': np.sum,
                                                                                    'payment_value': np.sum})\
                                                                              .reset_index()

#%%
# orders review
df_order_reviews = pd.read_parquet('olist_order_reviews_dataset.parquet', engine='pyarrow')

# selecionando order_id, rvw_score, rvw_commwent_message e preenchendo os valores nulls de review com "sem mensagem de review"
df_order_reviews = df_order_reviews[['order_id', 'review_id', 'review_score', 'review_comment_message']].fillna({'review_comment_message': 'sem mensagem de review'})

#%%
# df_customers
df_customers = pd.read_parquet('olist_customers_dataset.parquet', engine='pyarrow')

# convertendo a coluna de zip_code para str
df_customers['customer_zip_code_prefix'] = df_customers['customer_zip_code_prefix'].astype('str')

#%%
# df_sellers
df_sellers = pd.read_parquet('olist_sellers_dataset.parquet', engine='pyarrow')

# convertendo a coluna de zip_code para str
df_sellers['seller_zip_code_prefix'] = df_sellers['seller_zip_code_prefix'].astype('str')

#%%
# conectando (ou criando) um bd sqlite3 (bd local no diretorio do projeto)
con = sqlite3.connect('../Queries/olist_database.db')
cur = con.cursor()

#%%
# fazendo o load dos dataframes para o banco.
# Como o nome dados aos dataframes são iguais aos nomes dos arquivos sem as palavras 
# "olist" e "dataset", podemos criar uma lista com os nomes dos dfs e iterar sobre pra fazer o load
# para o banco

# o ultimo arquivo do diretorio nao possui o prefixo olist e o sufixo dataset. Como nao usamos, listdir vai até o penultimo parquet
lista_dfs = ['df_' + arquivo.removeprefix('olist_').removesuffix('_dataset.parquet') for arquivo in listdir()[:-1] if arquivo.endswith('.parquet')]

# removendo a string df_geolocation pois nao usamos
lista_dfs.remove('df_geolocation')

# criando uma lista com os pedidos cancelados para dropparmos de todas as tabelas que possuem esses order_id
orders_canceled = list(df_orders.query("order_status == 'canceled'")['order_id'])

# percorrendo a lista de arquivos. a fç globals()[nome_df] nos permite acessar a variavel em que cada df está
# armazenado. Isso nos permite acessar os dataframes a partir de seus nomes (strings) dinamicamente.
for nome_df in lista_dfs:
    df = globals()[nome_df]

    if 'order_id' in df.columns:
        df = df.query(f"order_id not in {orders_canceled}") # filtra fora os pedidos cancelados antes de carregar no banco

    # carrega para o banco fazendo o replace do prefixo 'df' por 'table_'
    df.to_sql(name=nome_df.replace("df_", "table_"), con=con, index=False, if_exists='replace')

# checando se os arquivos foram carregados para o banco
cur.execute("""SELECT
                tbl_name
               FROM sqlite_master
               WHERE type = 'table'""")\
    .fetchall()

# fechando conexao com banco
cur.close()
con.close()
