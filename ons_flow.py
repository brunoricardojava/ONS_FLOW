import requests
from datetime import datetime as dt
import pandas as pd
import json
import logging
import pathlib
import os

Current_dir = pathlib.Path().absolute()

url_EDW = '/home/ubuntu/Repository/studies/Decomp_runs/cases_output_base/EDW_Database/' + datetime.datetime.now().strftime("%Y%m%d")

log_path = "log/"

loop_qnt = 12

try:
    os.makedirs(log_path, exist_ok=True)

except Exception as e:
    print("Erro ao criar diretorio \"log\": ")
    print(e)
    quit()

else:
    logging.basicConfig(filename=log_path + "ons_flow_log.log", level=logging.INFO)
    logging.info(dt.datetime.now().strftime("%d_%m_%Y-%H:%M:%S: ") + "New process")

#------------------------------
# GET Real Time Energy Balance
#------------------------------

url = 'http://tr.ons.org.br/Content/GetBalancoEnergetico/null'

headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Origin': 'http://www.ons.org.br',
        'Referer': 'http://www.ons.org.br/paginas/energia-agora/balanco-de-energia'
          }
try:
    logging.info(datetime.datetime.now().strftime("%d_%m_%Y-%H:%M:%S: ") + "Fazendo a requisição")
    r = requests.get(url, headers=headers)
except:
    logging.error(dt.datetime.now().strftime("%d_%m_%Y-%H:%M:%S: ") + "Erro ao fazer requisição")
    logging.info(dt.datetime.now().strftime("%d_%m_%Y-%H:%M:%S: ") + "End of process")
    quit()

balance = {}

balance = r.json()

date = dt.strptime(balance['Data'][:16],'%Y-%m-%dT%H:%M') # local brazilian time

index = pd.date_range(date, periods=1)

df = pd.DataFrame(index = index)

logging.info(dt.datetime.now().strftime("%d_%m_%Y-%H:%M:%S: ") + "Extraindo os dados")
for i in balance['intercambio']:
    df[i] = balance['intercambio'][i]
    
for i in balance['internacional']:
    df[i] = balance['internacional'][i]    
    
regions = ['nordeste','norte','sudesteECentroOeste','sul']
gentype = ['eolica','hidraulica','nuclear','solar','termica','total']

for i in regions:
    for j in balance[i]:
        if j != 'geracao':
            df[i + '_' + j] = balance[i][j]
        if j == 'geracao':
            for t in balance[i][j]:
                df[i + '_' + j + '_' + t] =  balance[i][j][t]

# columns to english

tdic = {
        'internacional_sul' : 'BRA Flow Actual ONS Real Time S-Int',
        'sul_sudeste' : 'BRA Flow Actual ONS Real Time S-SE',
        'sudeste_nordeste' : 'BRA Flow Actual ONS Real Time SE-NE', 
        'sudeste_norteFic' : 'BRA Flow Actual ONS Real Time SE-FC',
        'norte_norteFic' : 'BRA Flow Actual ONS Real Time N-FC',
        'norteFic_nordeste' : 'BRA Flow Actual ONS Real Time FC-NE',
        'argentina' : 'BRA Flow Actual ONS Real Time Argentina',
        'paraguai' : 'BRA Flow Actual ONS Real Time Paraguai',
        'uruguai': 'BRA Flow Actual ONS Real Time Uruguai',
        'nordeste_geracao_total' : 'BRA Total Generation Actual ONS Real Time NE',
        'nordeste_geracao_hidraulica' : 'BRA Hydro Generation Actual ONS Real Time NE',
        'nordeste_geracao_termica' : 'BRA Thermal Generation Actual ONS Real Time NE',
        'nordeste_geracao_eolica' : 'BRA Wind Generation Actual ONS Real Time NE',
        'nordeste_geracao_nuclear' : 'BRA Nuclear Generation Actual ONS Real Time NE',
        'nordeste_geracao_solar' : 'BRA Solar Generation Actual ONS Real Time NE',
        'nordeste_cargaVerificada' : 'BRA Load Actual ONS Real Time NE',
        'nordeste_importacao' : 'BRA Flow Actual ONS Real Time NE Import',
        'nordeste_exportacao' : 'BRA Flow Actual ONS Real Time NE Export',
        'norte_geracao_total' : 'BRA Total Generation Actual ONS Real Time N',
        'norte_geracao_hidraulica' : 'BRA Hydro Generation Actual ONS Real Time N',
        'norte_geracao_termica' : 'BRA Thermal Generation Actual ONS Real Time N',
        'norte_geracao_eolica' : 'BRA Wind Generation Actual ONS Real Time N',
        'norte_geracao_nuclear' : 'BRA Nuclear Generation Actual ONS Real Time N',
        'norte_geracao_solar' : 'BRA Solar Generation Actual ONS Real Time N',
        'norte_cargaVerificada' : 'BRA Load Actual ONS Real Time N',
        'norte_importacao' : 'BRA Flow Actual ONS Real Time N Import',
        'norte_exportacao' : 'BRA Flow Actual ONS Real Time N Export',
        'sudesteECentroOeste_geracao_total' : 'BRA Total Generation Actual ONS Real Time SE',
        'sudesteECentroOeste_geracao_hidraulica' : 'BRA Hydro Generation Actual ONS Real Time SE',
        'sudesteECentroOeste_geracao_termica' : 'BRA Thermal Generation Actual ONS Real Time SE',
        'sudesteECentroOeste_geracao_eolica' : 'BRA Wind Generation Actual ONS Real Time SE',
        'sudesteECentroOeste_geracao_nuclear' : 'BRA Nuclear Generation Actual ONS Real Time SE',
        'sudesteECentroOeste_geracao_solar' : 'BRA Solar Generation Actual ONS Real Time SE',
        'sudesteECentroOeste_geracao_itaipu50HzBrasil' : 'BRA Hydro Generation Actual ONS Real Time Itaipu50HzBrasil',
        'sudesteECentroOeste_geracao_itaipu60Hz' : 'BRA Hydro Generation Actual ONS Real Time Itaipu60Hz',
        'sudesteECentroOeste_cargaVerificada' : 'BRA Load Actual ONS Real Time SE',
        'sudesteECentroOeste_importacao' : 'BRA Flow Actual ONS Real Time SE Import',
        'sudesteECentroOeste_exportacao' : 'BRA Flow Actual ONS Real Time SE Export',
        'sul_geracao_total' : 'BRA Total Generation Actual ONS Real Time S',
        'sul_geracao_hidraulica' : 'BRA Hydro Generation Actual ONS Real Time S',
        'sul_geracao_termica' : 'BRA Thermal Generation Actual ONS Real Time S',
        'sul_geracao_eolica' : 'BRA Wind Generation Actual ONS Real Time S',
        'sul_geracao_nuclear' : 'BRA Nuclear Generation Actual ONS Real Time S',
        'sul_geracao_solar' : 'BRA Solar Generation Actual ONS Real Time S',
        'sul_cargaVerificada' : 'BRA Load Actual ONS Real Time S',
        'sul_importacao' : 'BRA Flow Actual ONS Real Time S Import',
        'sul_exportacao' : 'BRA Flow Actual ONS Real Time S Export',
        }  


value_list = list(tdic.values())
series_dict = {}
for i in range(47):
    series = '1156948' + str(i + 14)
    series_dict[value_list[i]] = series

# renaming dataframe columns

cols = []

for col in list(df):
    cols.append(tdic[col])
    
df.columns = cols

#Transpose
df = df.T

df_final = pd.DataFrame({'Id' : [], 'ValueDate' : [], 'Value' : []})

for i in os.listdir(Current_dir + ):
    if (i == 'ONS_FLOW.csv'):
        logging.info(datetime.datetime.now().strftime("%d_%m_%Y-%H:%M:%S: ") + "Arquivo ONS_FLOW existente")
        
        try:
            logging.info(datetime.datetime.now().strftime("%d_%m_%Y-%H:%M:%S: ") + "Carregando arquivo ONS_FLOW")
            df_final = pd.read_csv('ONS_FLOW.csv')
        except:
            logging.error(dt.datetime.now().strftime("%d_%m_%Y-%H:%M:%S: ") + "Erro ao carregar arquivo")
            logging.info(dt.datetime.now().strftime("%d_%m_%Y-%H:%M:%S: ") + "End of process")
            quit()

        if(df_final.shape[0] >= loop_quant*47):
            logging.info(dt.datetime.now().strftime("%d_%m_%Y-%H:%M:%S: ") + "Arquivo csv local pronto para ser mandado para EDW.")
            try:
                logging.info(dt.datetime.now().strftime("%d_%m_%Y-%H:%M:%S: ") + "Mandando dados para a EDW!")
                df_final.to_csv(url_EDW + '/ONS_FLOW' + dt.datetime.now().strftime("%Y%m%d%H%M") + '.csv', index=False)
            except:
                logging.error(dt.datetime.now().strftime("%d_%m_%Y-%H:%M:%S: ") + "Erro ao enviar arquivo para EDW!")
                logging.info(dt.datetime.now().strftime("%d_%m_%Y-%H:%M:%S: ") + "End of process")
                quit()
            else:
                logging.info(dt.datetime.now().strftime("%d_%m_%Y-%H:%M:%S: ") + "Arquivo enviado.")
                try:
                    logging.info(dt.datetime.now().strftime("%d_%m_%Y-%H:%M:%S: ") + "Apagando arquivo.")
                    os.remove('ONS_FLOW.csv')
                except:
                    logging.error(dt.datetime.now().strftime("%d_%m_%Y-%H:%M:%S: ") + "Erro ao apagar arquivo!")
                    logging.info(dt.datetime.now().strftime("%d_%m_%Y-%H:%M:%S: ") + "End of process")
                    quit()
                else:
                    logging.info(dt.datetime.now().strftime("%d_%m_%Y-%H:%M:%S: ") + "Arquivo apagado!")

        else:
            logging.info(dt.datetime.now().strftime("%d_%m_%Y-%H:%M:%S: ") + "Coletando dados no arquivo csv local.")
        
        break

ValueDate = df.columns.tolist()[0].strftime("%d.%m.%Y %H:%M:%S")
for i in df.index:
    Id = series_dict[i]
    Value = df.loc[[i]][ValueDate].iloc[0]
    #print(Value)
    
    #Mount a Final Data Frame
    pd_series = pd.Series([Id, ValueDate, Value], index=['Id', 'ValueDate', 'Value'])
    df_final = df_final.append(pd_series, ignore_index=True)

df_final.to_csv('ONS_FLOW.csv', index=False)