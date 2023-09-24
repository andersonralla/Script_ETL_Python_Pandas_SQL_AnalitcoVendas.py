# Anderson R Oliveira / 23-09-23

# Scrip que faz a unificação automatica de diversos arquivos no mesmo padrao e realiza o insert no banco de dados e gera um arquivo consolidado em CSV.

#SugestaoUso: Reports diarios que necessitam ser unificados e armazenados/Automatização para analises dos dados.

# Todos os Dados dos arquivos sao ficticios.

print("Begin")

print("Inicio import")

import pandas as pd
from sqlalchemy import create_engine, text
import os
import chardet
import time
#import csv
start_time = time.time()
print("Fim import")

print("Inicio Conexao com o BD")

# Conexao com o BD

server_name = '????????' # <-- Inserir o seu Servidor aqui
database_name = 'develop_r4' # <-- Inserir o seu Banco de dados 
table_name = 'AnaliticoCompras' # <-- Inserir a sua Tabela aqui

connection_string = f"mssql+pyodbc://{server_name}/{database_name}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server"

engine = create_engine(connection_string)

print("Fim Conexao com o BD")


print("Inicio Indentificar o ECODING do arquivo")

# Indentificar o ECODING do arquivo

def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read())
    return result['encoding']

file_path = 'Arquivo1.csv'
encoding = detect_encoding(file_path)

print(f"{file_path} é {encoding}")

print("Fim Indentificar o ECODING do arquivo")

print("Inicio config diretorios")

#Diretorio

Dir = r'C:\Users\?????\Documentos\PythonRepo' # <---- Adicionar aqui o caminho da pasta onde esta os arquivos, é necessario deixar tudo no mesmo diretorio

SaveConcat = r'C:\Users\?????\Documentos\PythonRepo\SaveConcat'  # <---- Criar uma pasta interna no diretorio para salvar o arquivo da unificacao, para evitar que a partir da 2º leitura, entre o arquivo unificado no fluxo, evitando a duplicidade.

NomeArq = 'ArqConcat.csv'

caminhoPath = os.path.join(SaveConcat, NomeArq)

print("Fim config diretorios")

print("Inicio ETL")

#ETL

arquivos_csv = [arquivo for arquivo in os.listdir(Dir) if arquivo.endswith('.csv')]

df_concat = pd.DataFrame()

for arquivo_csv in arquivos_csv:

    df = pd.read_csv(arquivo_csv, sep=';')

    df['CpfCliente'] = df['CpfCliente'].apply(lambda x: f'{x:011}') # <--- Formata a coluna CPF para que nao exclua o zero a esquerda.

    df_concat = pd.concat([df_concat, df], ignore_index=True)

#display(df_concat)

#resultado = df_concat[df_concat['CpfCliente'] == "47540001111"]

#display(resultado)

#len(arquivos_csv)

print("Fim ETL")

print("Inicio Insert no Banco de Dados")

#Le o arquivo

print(df_concat)

#Salva arquivo diretorio

df_concat.to_csv(caminhoPath, index=False)

#Insere no BD
df_concat.to_sql(table_name, engine, if_exists='replace', index=False)

print("Fim Insert no Banco de Dados")

engine.dispose()

print("End")

end_time = time.time()
total_duration = end_time - start_time
print(f" Finalizado com sucesso! Tempo total de execução: {total_duration:.2f} segundos")