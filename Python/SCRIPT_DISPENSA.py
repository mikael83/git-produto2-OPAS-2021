#!/usr/bin/env python
# coding: utf-8

# # Script Python - Jupyter - Banco de Dispensa  

# # 01/09/21 por Mikael Lemos - V1.0 

# ## Carregando as bibliotecas

# In[1]:


import os
import openpyxl
import black 
import numpy as np
import pandas as pd
#import pypolars as pl
import pyreadstat
from datetime import datetime, date, time, timezone
import pyflowchart
import vaex
from pathlib import Path
import h5py


# ## Carregar arquivo hdf5

# In[2]:


tb_dispensas_esquemas_udm = vaex.open('tb_dispensas_esquemas_udm.hdf5')


# ## Verificar categoria de dispensa - Colocar apenas dispensas O ou G

# In[3]:


tb_dispensas_esquemas_udm['categoria_disp'].value_counts()


# ## Caso não existam apenas dispensas do tipo O ou G - filtro para dispensas do tipo O ou G

# In[4]:


tb_dispensas_esquemas_udm['categoria_disp'] = tb_dispensas_esquemas_udm['categoria_disp'].astype(str)
tb_dispensas_esquemas_udm = tb_dispensas_esquemas_udm[tb_dispensas_esquemas_udm['categoria_disp'].str.contains("O|G")]


# ## Passo 1: Criando as variáveis de data 

# ## Manipulando formatos de data

# In[5]:


tb_dispensas_esquemas_udm['data_dispensa'] = tb_dispensas_esquemas_udm['data_dispensa'].astype('datetime64')
tb_dispensas_esquemas_udm['data_dispensa_anterior'] = tb_dispensas_esquemas_udm['data_dispensa_anterior'].astype('datetime64')
tb_dispensas_esquemas_udm['data_dispensa_posterior'] = tb_dispensas_esquemas_udm['data_dispensa_posterior'].astype('datetime64')
tb_dispensas_esquemas_udm['data_PriDisp'] = tb_dispensas_esquemas_udm['data_PriDisp'].astype('datetime64')
tb_dispensas_esquemas_udm['data_ultima_dispensa'] = tb_dispensas_esquemas_udm['data_ultima_dispensa'].astype('datetime64')


# In[6]:


tb_dispensas_esquemas_udm['data_dispensa'] = tb_dispensas_esquemas_udm['data_dispensa'].astype('datetime64')
tb_dispensas_esquemas_udm['data_dispensa_anterior'] = tb_dispensas_esquemas_udm['data_dispensa_anterior'].astype('datetime64')
tb_dispensas_esquemas_udm['data_dispensa_posterior'] = tb_dispensas_esquemas_udm['data_dispensa_posterior'].astype('datetime64')
tb_dispensas_esquemas_udm['data_PriDisp'] = tb_dispensas_esquemas_udm['data_PriDisp'].astype('datetime64')
tb_dispensas_esquemas_udm['data_ultima_dispensa'] = tb_dispensas_esquemas_udm['data_ultima_dispensa'].astype('datetime64')


# ## Criação das variáveis: dt_disp, anodisp,dt_disp_anterior, anodisp_anterior, dtdisp_mes, dt_Pridisp

# In[7]:


tb_dispensas_esquemas_udm['dt_disp'] = tb_dispensas_esquemas_udm['data_dispensa'].dt.strftime('%Y-%m-%d')
tb_dispensas_esquemas_udm['anodisp'] = tb_dispensas_esquemas_udm['data_dispensa'].dt.strftime('%Y')
tb_dispensas_esquemas_udm['dt_disp_anterior'] = tb_dispensas_esquemas_udm['data_dispensa_anterior'].dt.strftime('%Y-%m-%d')
tb_dispensas_esquemas_udm['anodisp_anterior'] = tb_dispensas_esquemas_udm['data_dispensa_anterior'].dt.strftime('%Y')
tb_dispensas_esquemas_udm['dt_disp_posterior'] = tb_dispensas_esquemas_udm['data_dispensa_posterior'].dt.strftime('%Y-%m-%d')
tb_dispensas_esquemas_udm['anodisp_posterior'] = tb_dispensas_esquemas_udm['data_dispensa_posterior'].dt.strftime('%Y')
tb_dispensas_esquemas_udm['dt_Ultdisp'] = tb_dispensas_esquemas_udm['data_ultima_dispensa'].dt.strftime('%Y-%m-%d')
tb_dispensas_esquemas_udm['AnoUltDisp'] = tb_dispensas_esquemas_udm['data_ultima_dispensa'].dt.strftime('%Y')
tb_dispensas_esquemas_udm['dt_Pridisp'] = tb_dispensas_esquemas_udm['data_PriDisp'].dt.strftime('%Y-%m-%d')
tb_dispensas_esquemas_udm['AnoPriDisp'] = tb_dispensas_esquemas_udm['data_PriDisp'].dt.strftime('%Y')
tb_dispensas_esquemas_udm['dtdisp_mes'] = tb_dispensas_esquemas_udm['data_dispensa'].dt.strftime('%m-%Y')


# In[8]:


tb_dispensas_esquemas_udm['dt_disp'] = tb_dispensas_esquemas_udm['data_dispensa'].dt.strftime('%Y-%m-%d')
tb_dispensas_esquemas_udm['anodisp'] = tb_dispensas_esquemas_udm['data_dispensa'].dt.strftime('%Y')
tb_dispensas_esquemas_udm['dt_disp_anterior'] = tb_dispensas_esquemas_udm['data_dispensa_anterior'].dt.strftime('%Y-%m-%d')
tb_dispensas_esquemas_udm['anodisp_anterior'] = tb_dispensas_esquemas_udm['data_dispensa_anterior'].dt.strftime('%Y')
tb_dispensas_esquemas_udm['dt_disp_posterior'] = tb_dispensas_esquemas_udm['data_dispensa_posterior'].dt.strftime('%Y-%m-%d')
tb_dispensas_esquemas_udm['anodisp_posterior'] = tb_dispensas_esquemas_udm['data_dispensa_posterior'].dt.strftime('%Y')
tb_dispensas_esquemas_udm['dt_Ultdisp'] = tb_dispensas_esquemas_udm['data_ultima_dispensa'].dt.strftime('%Y-%m-%d')
tb_dispensas_esquemas_udm['AnoUltDisp'] = tb_dispensas_esquemas_udm['data_ultima_dispensa'].dt.strftime('%Y')
tb_dispensas_esquemas_udm['dt_Pridisp'] = tb_dispensas_esquemas_udm['data_PriDisp'].dt.strftime('%Y-%m-%d')
tb_dispensas_esquemas_udm['AnoPriDisp'] = tb_dispensas_esquemas_udm['data_PriDisp'].dt.strftime('%Y')
tb_dispensas_esquemas_udm['dtdisp_mes'] = tb_dispensas_esquemas_udm['data_dispensa'].dt.strftime('%m-%Y')


# ## Criar variáveis : dt_disp_min e dt_disp_max

# In[9]:


tb_dispensas_esquemas_udm['dt_disp_min'] = tb_dispensas_esquemas_udm['dt_Pridisp']
tb_dispensas_esquemas_udm['dt_disp_max'] = tb_dispensas_esquemas_udm['dt_Ultdisp'] 


# ## Verificar variáveis 

# In[10]:


tb_dispensas_esquemas_udm.info()


# ## Unificar colunas para criar variável da última dispensa de cada ano - UltDispAno

# In[11]:


def combine(a,b,c,d,e,f,g,h,i,j,k,l,m):
    return a +  b + c +  d +  e +  f +  g +  h +  i +  j +  k +  l +  m 


# In[12]:


tb_dispensas_esquemas_udm['UltDispAno'] = combine(tb_dispensas_esquemas_udm.UltDisp_2009,tb_dispensas_esquemas_udm.UltDisp_2010,tb_dispensas_esquemas_udm.UltDisp_2011,tb_dispensas_esquemas_udm.UltDisp_2012,tb_dispensas_esquemas_udm.UltDisp_2013, tb_dispensas_esquemas_udm.UltDisp_2014,tb_dispensas_esquemas_udm.UltDisp_2015,tb_dispensas_esquemas_udm.UltDisp_2016,tb_dispensas_esquemas_udm.UltDisp_2017,tb_dispensas_esquemas_udm.UltDisp_2018,tb_dispensas_esquemas_udm.UltDisp_2019,tb_dispensas_esquemas_udm.UltDisp_2020,tb_dispensas_esquemas_udm.UltDisp_2021)


# ## Checar última dispensa ano - UltDispAno

# In[13]:


tb_dispensas_esquemas_udm['UltDispAno'].value_counts()


# ## Remover variáveis/colunas não utilizadas

# In[14]:


colunas_drop = [tb_dispensas_esquemas_udm.data_dispensa, tb_dispensas_esquemas_udm.data_dispensa_anterior, tb_dispensas_esquemas_udm.data_dispensa_posterior,tb_dispensas_esquemas_udm.data_PriDisp,tb_dispensas_esquemas_udm.data_ultima_dispensa, tb_dispensas_esquemas_udm.UltDisp_2009, tb_dispensas_esquemas_udm.UltDisp_2010, tb_dispensas_esquemas_udm.UltDisp_2011, tb_dispensas_esquemas_udm.UltDisp_2012, tb_dispensas_esquemas_udm.UltDisp_2013, tb_dispensas_esquemas_udm.UltDisp_2014, tb_dispensas_esquemas_udm.UltDisp_2015, tb_dispensas_esquemas_udm.UltDisp_2016, tb_dispensas_esquemas_udm.UltDisp_2017, tb_dispensas_esquemas_udm.UltDisp_2018, tb_dispensas_esquemas_udm.UltDisp_2019, tb_dispensas_esquemas_udm.UltDisp_2020, tb_dispensas_esquemas_udm.UltDisp_2021]


# In[15]:


tb_dispensas_esquemas_udm  = tb_dispensas_esquemas_udm.drop(colunas_drop, inplace=False,  check=True ) 


# ## Variável PriDisp_arpp

# In[16]:


tb_dispensas_esquemas_udm['PriDisp_arpp'] = tb_dispensas_esquemas_udm['PriDisp']


# In[17]:


tb_dispensas_esquemas_udm['PriDisp_arpp'] = tb_dispensas_esquemas_udm.PriDisp_arpp.astype('str')


# In[18]:


datas_ig = tb_dispensas_esquemas_udm.dt_disp == tb_dispensas_esquemas_udm.dt_disp_min


# In[19]:


if datas_ig is True:
   tb_dispensas_esquemas_udm['PriDisp_arpp'] = tb_dispensas_esquemas_udm.PriDisp_arpp.str.title().str.replace('0', '1')
else:
   tb_dispensas_esquemas_udm['PriDisp_arpp'] = tb_dispensas_esquemas_udm.PriDisp_arpp.str.title().str.replace('0', '0')


# In[20]:


tb_dispensas_esquemas_udm['PriDisp_arpp'].value_counts()


# # Crosstabs - arquivo: 1 Informações Gerais - sheet: Gerais Agosto 2021

# ## Crosstabs variáveis: anodisp * UltDispAno 

# In[21]:


dispensa_crosstab =  tb_dispensas_esquemas_udm['anodisp','UltDispAno']


# In[22]:


dispensa_crosstab


# In[23]:


dispensa_crosstab = dispensa_crosstab.to_pandas_df(['anodisp','UltDispAno'])


# In[24]:


dispensa_crosstab = dispensa_crosstab.groupby(['anodisp','UltDispAno']).size().reset_index(name='Total')


# In[25]:


table_crosstab = pd.pivot_table(dispensa_crosstab, values='Total', index=['anodisp'],
                    columns=[ 'UltDispAno'], aggfunc=np.sum)


# In[26]:


table_crosstab


# ## Soma das colunas

# In[27]:


table_crosstab['Total'] = table_crosstab.loc[:, 0:].apply(np.sum, axis=1)


# ## Soma de linhas de cada coluna

# In[28]:


total = table_crosstab.sum()
total.name = 'Total'
table_crosstab = table_crosstab.append(total.transpose())


# ## Crosstabs variáveis: anodisp * PriDisp 

# In[29]:


dispensa_crosstab2 =  tb_dispensas_esquemas_udm['anodisp','PriDisp']


# In[30]:


dispensa_crosstab2


# In[31]:


dispensa_crosstab2 = dispensa_crosstab2.to_pandas_df(['anodisp','PriDisp'])


# In[32]:


dispensa_crosstab2 = dispensa_crosstab2.groupby(['anodisp','PriDisp']).size().reset_index(name='Total')


# In[33]:


table_crosstab2 = pd.pivot_table(dispensa_crosstab2, values='Total', index=['anodisp'],
                    columns=[ 'PriDisp'], aggfunc=np.sum)


# In[34]:


table_crosstab2


# ## Soma das colunas

# In[35]:


table_crosstab2['Total'] = table_crosstab2.loc[:, 0:].apply(np.sum, axis=1)


# ## Soma das linhas de cada coluna

# In[36]:


total = table_crosstab2.sum()
total.name = 'Total'
table_crosstab2 = table_crosstab2.append(total.transpose())


# ## Tabela de frequência / Porcentagem de duplicados/não duplicados com base na ultima dispensa da vida (UltDispVida)

# In[37]:


dispensa_crosstab3 =  tb_dispensas_esquemas_udm['codigo_paciente','UltDispVida']


# In[38]:


dispensa_crosstab3 = dispensa_crosstab3.to_pandas_df(['codigo_paciente','UltDispVida'])


# In[39]:


dispensa_crosstab3 = pd.DataFrame(dispensa_crosstab3.value_counts())


# In[40]:


dispensa_crosstab3


# ## Separar primary case '0' = 1

# In[41]:


dispensa_crosstab3 = dispensa_crosstab3.rename(columns={0: 'Frequency'})


# In[42]:


dispensa_crosstab3_primary =  dispensa_crosstab3.query('Frequency == 1')


# In[43]:


total = dispensa_crosstab3_primary.sum()
total.name = 'Primary Case'
dispensa_crosstab3_primary = dispensa_crosstab3_primary.append(total.transpose())


# In[44]:


dispensa_crosstab3_primary = dispensa_crosstab3_primary.iloc[[-1]]


# In[45]:


dispensa_crosstab3_primary


# ## Separar duplicate case '0' > 1

# In[46]:


dispensa_crosstab3_duplicate =  dispensa_crosstab3.query('Frequency > 1')


# In[47]:


total = dispensa_crosstab3_duplicate.sum()
total.name = 'Duplicate Case'
dispensa_crosstab3_duplicate = dispensa_crosstab3_duplicate.append(total.transpose())


# In[48]:


dispensa_crosstab3_duplicate = dispensa_crosstab3_duplicate.iloc[[-1]]


# In[49]:


dispensa_crosstab3_duplicate


# ## Unir tabelas para criar 'Indicator of each last matching case as Primary' - UltDispVida

# In[50]:


table_crosstab3 = pd.concat([dispensa_crosstab3_primary, dispensa_crosstab3_duplicate])


# In[51]:


table_crosstab3


# ## Coluna de Porcentagem

# In[52]:


table_crosstab3['Percent'] = (table_crosstab3['Frequency'] / table_crosstab3['Frequency'].sum()) * 100


# In[53]:


table_crosstab3['Percent'] = pd.Series([round(val, 1) for val in table_crosstab3['Percent']], index = table_crosstab3.index)


# ## Calculando total

# In[54]:


total = table_crosstab3.sum()
total.name = 'Total'
table_crosstab3 = table_crosstab3.append(total.transpose())


# ## Converter colunas de float64 para int32

# In[55]:


table_crosstab3['Frequency'] = np.nan_to_num(table_crosstab3['Frequency']).astype(int)


# # Crosstabs - arquivo: 1 Informações Gerais - sheet: Gerais - Crosstabs CD4 - tabela: tb_cd4_consolidado.txt

# ## Carregar arquivo hdf5

# In[56]:


tb_cd4_consolidado = vaex.open('tb_cd4_consolidado.hdf5')


# In[57]:


tb_cd4_consolidado.info()


# ## Passo 1 : Criando Variável de data

# In[58]:


tb_cd4_consolidado[ "('data_hora_coleta',)"] = tb_cd4_consolidado[ "('data_hora_coleta',)"].astype('datetime64')


# In[59]:


tb_cd4_consolidado[ "('data_hora_coleta',)"] = tb_cd4_consolidado[ "('data_hora_coleta',)"].astype('datetime64')


# In[60]:


tb_cd4_consolidado['anocoleta'] = tb_cd4_consolidado["('data_hora_coleta',)"].dt.strftime('%Y')


# In[61]:


tb_cd4_consolidado['anocoleta'] = tb_cd4_consolidado["('data_hora_coleta',)"].dt.strftime('%Y')


# ## Unificar colunas para criar variável da última dispensa de cada ano - UltCD4Ano

# In[62]:


def combine(a,b,c,d,e,f,g,h,i,j,k,l,m):
    return a +  b + c +  d +  e +  f +  g +  h +  i +  j +  k +  l +  m 


# In[63]:


tb_cd4_consolidado['UltCD4Ano'] = combine(tb_cd4_consolidado["('UltCD4_2009',)"] ,tb_cd4_consolidado["('UltCD4_2010',)"],tb_cd4_consolidado["('UltCD4_2011',)"],tb_cd4_consolidado["('UltCD4_2012',)"],tb_cd4_consolidado["('UltCD4_2013',)"], tb_cd4_consolidado["('UltCD4_2014',)"],tb_cd4_consolidado["('UltCD4_2015',)"],tb_cd4_consolidado["('UltCD4_2016',)"],tb_cd4_consolidado["('UltCD4_2017',)"],tb_cd4_consolidado["('UltCD4_2018',)"],tb_cd4_consolidado["('UltCD4_2019',)"],tb_cd4_consolidado["('UltCD4_2020',)"],tb_cd4_consolidado["('UltCD4_2021',)"])


# ## Crosstabs variáveis: anocoleta * UltCD4Ano 

# In[64]:


cd4_crosstab =  tb_cd4_consolidado['anocoleta','UltCD4Ano']


# In[65]:


cd4_crosstab


# In[66]:


cd4_crosstab = cd4_crosstab.to_pandas_df(['anocoleta','UltCD4Ano'])


# In[67]:


cd4_crosstab = cd4_crosstab.groupby(['anocoleta','UltCD4Ano']).size().reset_index(name='Total')


# In[68]:


cd4_crosstab = pd.pivot_table(cd4_crosstab, values='Total', index=['anocoleta'],
                    columns=[ 'UltCD4Ano'], aggfunc=np.sum)


# In[69]:


cd4_crosstab.info()


# In[70]:


cd4_crosstab['Total'] = cd4_crosstab.loc[:, 0:].apply(np.sum, axis=1)


# In[71]:


total = cd4_crosstab.sum()
total.name = 'Total'
cd4_crosstab = cd4_crosstab.append(total.transpose())


# ## Converter colunas de float64 para int32

# In[72]:


cd4_crosstab[[1]] = np.nan_to_num(cd4_crosstab[[1]]).astype(int)
cd4_crosstab[[0]] = np.nan_to_num(cd4_crosstab[[0]]).astype(int)
cd4_crosstab['Total'] = np.nan_to_num(cd4_crosstab['Total']).astype(int)


# In[73]:


cd4_crosstab


# ## Crosstabs variáveis: anocoleta * PriCD4 

# In[74]:


cd4_crosstab2 =  tb_cd4_consolidado['anocoleta',"('PriCD4',)" ]


# In[75]:


cd4_crosstab2


# In[76]:


cd4_crosstab2 = cd4_crosstab2.to_pandas_df(['anocoleta',"('PriCD4',)"])


# In[77]:


cd4_crosstab2 = cd4_crosstab2.groupby(['anocoleta',"('PriCD4',)"]).size().reset_index(name='Total')


# In[78]:


cd4_crosstab2 = pd.pivot_table(cd4_crosstab2, values='Total', index=['anocoleta'],
                    columns=["('PriCD4',)"], aggfunc=np.sum)


# In[79]:


cd4_crosstab2['Total'] = cd4_crosstab2.loc[:, 0:].apply(np.sum, axis=1)


# In[80]:


total = cd4_crosstab2.sum()
total.name = 'Total'
cd4_crosstab2 = cd4_crosstab2.append(total.transpose())


# In[81]:


tb_cd4_consolidado.info()


# ## Criando variáveis: dt_disp_min, diasColetaDispensa2, dt_coleta - a partir de variáveis de data - transformar variáveis em para datetime

# In[82]:


cd4_crosstab3 = tb_cd4_consolidado.to_pandas_df(["('data_hora_coleta',)","('dt_primeira_dispensa',)", "('PriCD4',)" ])


# In[83]:


cd4_crosstab3[ "('dt_primeira_dispensa',)"] = cd4_crosstab3[ "('dt_primeira_dispensa',)"].astype('datetime64')


# In[84]:


cd4_crosstab3[ "('dt_primeira_dispensa',)"] = cd4_crosstab3[ "('dt_primeira_dispensa',)"].astype('datetime64')


# In[85]:


cd4_crosstab3


# ## Criação das variáveis

# In[86]:


cd4_crosstab3['dt_coleta'] = cd4_crosstab3["('data_hora_coleta',)"].dt.strftime('%Y-%m-%d')
cd4_crosstab3['dt_disp_min'] = cd4_crosstab3["('dt_primeira_dispensa',)"].dt.strftime('%Y-%m-%d')


# In[87]:


cd4_crosstab3['dt_coleta'] = pd.to_datetime(cd4_crosstab3['dt_coleta'])
cd4_crosstab3['dt_disp_min'] = pd.to_datetime(cd4_crosstab3['dt_disp_min'])


# In[88]:


cd4_crosstab3['diasColetaDispensa2'] = (cd4_crosstab3['dt_coleta'] - cd4_crosstab3['dt_disp_min']).dt.days


# In[89]:


cd4_crosstab3['diasColetaDispensa2'] = cd4_crosstab3['diasColetaDispensa2'].fillna(999999999)


# In[90]:


cd4_crosstab3['diasColetaDispensa2'].value_counts()


# In[91]:


cd4_crosstab3


# ## Correções na coluna: diasColetaDispensa2 - int32 

# In[92]:


cd4_crosstab3['diasColetaDispensa2'] = np.nan_to_num(cd4_crosstab3['diasColetaDispensa2']).astype(int)


# ## módulo dos valores

# In[93]:


#cd4_crosstab3['diasColetaDispensa2'] = cd4_crosstab3['diasColetaDispensa2'].abs()  


# ## Criar variável : priCD4_antesTARV

# In[94]:


cd4_crosstab3['priCD4_antesTARV'] = 0


# ## Substituir 0 na variável priCD4_antesTARV de acordo com as condições:

# In[95]:


cd4_crosstab3 = cd4_crosstab3.rename(columns={"('PriCD4',)": "PriCD4"})


# In[96]:


cd4_crosstab3.loc[(cd4_crosstab3['diasColetaDispensa2'] <= 15) & (cd4_crosstab3['PriCD4'] == 1), 'priCD4_antesTARV'] = 1  
cd4_crosstab3.loc[(cd4_crosstab3['diasColetaDispensa2'] == 999999999) & (cd4_crosstab3['PriCD4'] == 1), 'priCD4_antesTARV'] = 1  


# ## Tabela anocoleta * priCD4_antesTARV

# In[97]:


cd4_crosstab3['anocoleta'] = cd4_crosstab3["('data_hora_coleta',)"].dt.strftime('%Y')


# In[98]:


cd4_crosstab3 = cd4_crosstab3.groupby(['anocoleta','priCD4_antesTARV']).size().reset_index(name='Total')


# In[99]:


cd4_crosstab3


# In[100]:


cd4_crosstab3 = pd.pivot_table(cd4_crosstab3, values='Total', index=['anocoleta'],
                    columns=["priCD4_antesTARV"], aggfunc=np.sum)


# In[101]:


cd4_crosstab3['Total'] = cd4_crosstab3.loc[:, 0:].apply(np.sum, axis=1)


# In[102]:


total = cd4_crosstab3.sum()
total.name = 'Total'
cd4_crosstab3 = cd4_crosstab3.append(total.transpose())


# ## Identificação de casos duplicados - variáveis : cod_pac_final, CD4maispertoPriDisp, dt_coleta

# In[103]:


cd4_crosstab4 = tb_cd4_consolidado.to_pandas_df(["('cod_pac_final',)","('data_hora_coleta',)","('dt_primeira_dispensa',)", "('PriCD4',)" ])


# In[104]:


cd4_crosstab4[ "('dt_primeira_dispensa',)"] = cd4_crosstab4[ "('dt_primeira_dispensa',)"].astype('datetime64')


# In[105]:


cd4_crosstab4[ "('dt_primeira_dispensa',)"] = cd4_crosstab4[ "('dt_primeira_dispensa',)"].astype('datetime64')


# In[106]:


cd4_crosstab4['dt_coleta'] = cd4_crosstab4["('data_hora_coleta',)"].dt.strftime('%Y-%m-%d')
cd4_crosstab4['dt_disp_min'] = cd4_crosstab4["('dt_primeira_dispensa',)"].dt.strftime('%Y-%m-%d')


# In[107]:


cd4_crosstab4['dt_coleta'] = pd.to_datetime(cd4_crosstab4['dt_coleta'])
cd4_crosstab4['dt_disp_min'] = pd.to_datetime(cd4_crosstab4['dt_disp_min'])


# In[108]:


cd4_crosstab4.info()


# In[109]:


cd4_crosstab4['diasColetaDispensa2'] = (cd4_crosstab4['dt_coleta'] - cd4_crosstab4['dt_disp_min']).dt.days


# In[110]:


cd4_crosstab4['diasColetaDispensa2'] = cd4_crosstab4['diasColetaDispensa2'].fillna(999999999)


# ## diasColetaDispensa2 como int32

# In[111]:


cd4_crosstab4['diasColetaDispensa2'] = np.nan_to_num(cd4_crosstab4['diasColetaDispensa2']).astype(int)


# ## Criar variável : CD4maispertoPriDisp_ano

# In[112]:


cd4_crosstab4['CD4maispertoPriDisp_ano'] = 0


# ## Substituir 0 na variável CD4maispertoPriDisp_ano de acordo com as condições:

# In[113]:


cd4_crosstab4 = cd4_crosstab4.rename(columns={"('PriCD4',)": "PriCD4"})


# In[114]:


cd4_crosstab4 = cd4_crosstab4.rename(columns={"('cod_pac_final',)": "cod_pac_final"})


# In[115]:


cd4_crosstab4.loc[(cd4_crosstab4['diasColetaDispensa2'] <= 15) & (cd4_crosstab4['diasColetaDispensa2'] >= -180), 'CD4maispertoPriDisp_ano'] = 1  
cd4_crosstab4.loc[(cd4_crosstab4['diasColetaDispensa2'] <= 15) & (cd4_crosstab4['diasColetaDispensa2'] >= -365), 'CD4maispertoPriDisp_ano'] = 1  


# ## Criar tabela de casos duplicados cod_pac_final*CD4maispertoPriDisp_ano

# In[116]:


cd4_crosstab5 =  cd4_crosstab4.drop(columns=["('data_hora_coleta',)", "('dt_primeira_dispensa',)", 'PriCD4','dt_disp_min', 'dt_coleta', 'diasColetaDispensa2' ])


# In[117]:


cd4_crosstab5


# In[118]:


cd4_crosstab5 = pd.DataFrame(cd4_crosstab5.value_counts())


# ## Separar primary case "0"/Frequency = 1

# In[119]:


cd4_crosstab5 = cd4_crosstab5.rename(columns={0: 'Frequency'})


# In[120]:


cd4_crosstab5_primary =  cd4_crosstab5.query('Frequency == 1')


# In[121]:


total = cd4_crosstab5_primary.sum()
total.name = 'Primary Case'
cd4_crosstab5_primary = cd4_crosstab5_primary.append(total.transpose())


# In[122]:


cd4_crosstab5_primary = cd4_crosstab5_primary.iloc[[-1]]


# In[123]:


cd4_crosstab5_primary


# ## Separar duplicate case "0"/Frequency > 1

# In[124]:


cd4_crosstab5_duplicate =  cd4_crosstab5.query('Frequency > 1')


# In[125]:


total = cd4_crosstab5_duplicate.sum()
total.name = 'Duplicate Case'
cd4_crosstab5_duplicate = cd4_crosstab5_duplicate.append(total.transpose())


# In[126]:


cd4_crosstab5_duplicate = cd4_crosstab5_duplicate.iloc[[-1]]


# In[127]:


cd4_crosstab5_duplicate


# ## Unir tabelas e criar o indicador primary/duplicate cases -  cod_pac_final*CD4maispertoPriDisp_ano

# In[128]:


cd4_crosstab5 = pd.concat([cd4_crosstab5_primary, cd4_crosstab5_duplicate])


# In[129]:


cd4_crosstab5


# In[130]:


cd4_crosstab5['Percent'] = (cd4_crosstab5['Frequency'] / cd4_crosstab5['Frequency'].sum()) * 100


# In[131]:


cd4_crosstab5['Percent'] = pd.Series([round(val, 1) for val in cd4_crosstab5['Percent']], index = cd4_crosstab5.index)


# In[132]:


total = cd4_crosstab5.sum()
total.name = 'Total'
cd4_crosstab5 = cd4_crosstab5.append(total.transpose())


# ## Converter coluna Frequency de float para int32

# In[133]:


cd4_crosstab5['Frequency'] = np.nan_to_num(cd4_crosstab5['Frequency']).astype(int)


# # Crosstabs - arquivo: 1 Informações Gerais - sheet: Gerais Agosto 2021 - Crosstabs CV - tabela: tb_carga_viral_consolidado.txt

# In[134]:


tb_carga_viral_consolidado = vaex.open('tb_carga_viral_consolidado.hdf5')


# In[135]:


tb_carga_viral_consolidado.info()


# ## Passo 1: Criando variável de data

# In[136]:


tb_carga_viral_consolidado[ "('data_hora_coleta',)"] = tb_carga_viral_consolidado[ "('data_hora_coleta',)"].astype('datetime64')


# In[137]:


tb_carga_viral_consolidado[ "('data_hora_coleta',)"] = tb_carga_viral_consolidado[ "('data_hora_coleta',)"].astype('datetime64')


# In[138]:


tb_carga_viral_consolidado['anocoleta'] = tb_carga_viral_consolidado["('data_hora_coleta',)"].dt.strftime('%Y')


# In[139]:


tb_carga_viral_consolidado['anocoleta'] = tb_carga_viral_consolidado["('data_hora_coleta',)"].dt.strftime('%Y')


# ## Criação da variável : UltCVAno

# In[140]:


def combine(a,b,c,d,e,f,g,h,i,j,k,l,m):
    return a +  b + c +  d +  e +  f +  g +  h +  i +  j +  k +  l +  m 


# In[141]:


tb_carga_viral_consolidado['UltCVAno'] = combine(tb_carga_viral_consolidado["('UltCV_2009',)"] ,tb_carga_viral_consolidado["('UltCV_2010',)"],tb_carga_viral_consolidado["('UltCV_2011',)"],tb_carga_viral_consolidado["('UltCV_2012',)"],tb_carga_viral_consolidado["('UltCV_2013',)"], tb_carga_viral_consolidado["('UltCV_2014',)"],tb_carga_viral_consolidado["('UltCV_2015',)"],tb_carga_viral_consolidado["('UltCV_2016',)"],tb_carga_viral_consolidado["('UltCV_2017',)"],tb_carga_viral_consolidado["('UltCV_2018',)"],tb_carga_viral_consolidado["('UltCV_2019',)"],tb_carga_viral_consolidado["('UltCV_2020',)"],tb_carga_viral_consolidado["('UltCV_2021',)"])


# ## Crosstabs CV: anocoleta*UltCVAno

# In[142]:


cv_crosstab =  tb_carga_viral_consolidado['anocoleta','UltCVAno']


# In[143]:


cv_crosstab = cv_crosstab.to_pandas_df(['anocoleta','UltCVAno'])


# In[144]:


cv_crosstab = cv_crosstab.groupby(['anocoleta','UltCVAno']).size().reset_index(name='Total')


# In[145]:


cv_crosstab = pd.pivot_table(cv_crosstab, values='Total', index=['anocoleta'],
                    columns=[ 'UltCVAno'], aggfunc=np.sum)


# In[146]:


cv_crosstab


# In[147]:


cv_crosstab['Total'] = cv_crosstab.loc[:, 0:].apply(np.sum, axis=1)


# In[148]:


total = cv_crosstab.sum()
total.name = 'Total'
cv_crosstab = cv_crosstab.append(total.transpose())


# ## Converter de float para int32

# In[149]:


cv_crosstab[[1]] = np.nan_to_num(cv_crosstab[[1]]).astype(int)
cv_crosstab[[0]] = np.nan_to_num(cv_crosstab[[0]]).astype(int)
cv_crosstab['Total'] = np.nan_to_num(cv_crosstab['Total']).astype(int)


# ## Crosstabs CV: anocoleta * PriCV

# In[150]:


cv_crosstab2 =  tb_carga_viral_consolidado['anocoleta',"('PriCV',)" ]


# In[151]:


cv_crosstab2 = cv_crosstab2.to_pandas_df(['anocoleta',"('PriCV',)"])


# In[152]:


cv_crosstab2 = cv_crosstab2.groupby(['anocoleta',"('PriCV',)"]).size().reset_index(name='Total')


# In[153]:


cv_crosstab2 = pd.pivot_table(cv_crosstab2, values='Total', index=['anocoleta'],
                    columns=["('PriCV',)"], aggfunc=np.sum)


# In[154]:


cv_crosstab2['Total'] = cv_crosstab2.loc[:, 0:].apply(np.sum, axis=1)


# In[155]:


total = cv_crosstab2.sum()
total.name = 'Total'
cv_crosstab2 = cv_crosstab2.append(total.transpose())


# In[156]:


cv_crosstab2[[1]] = np.nan_to_num(cv_crosstab2[[1]]).astype(int)
cv_crosstab2[[0]] = np.nan_to_num(cv_crosstab2[[0]]).astype(int)
cv_crosstab2['Total'] = np.nan_to_num(cv_crosstab2['Total']).astype(int)


# In[157]:


cv_crosstab2


# ## Salvar tabelas no arquivo excel: 1 Indicadores / sheet: Gerais

# In[158]:


with pd.ExcelWriter('1 Indicadores Agosto2021.xlsx') as writer:  
    table_crosstab.to_excel(writer, sheet_name='Gerais Agosto2021', startcol=3 ,startrow= 72, index_label='anodisp/UltDispAno')
    table_crosstab2.to_excel(writer, sheet_name='Gerais Agosto2021', startcol=8 ,startrow= 72, index_label= 'anodisp/PriDisp')
    table_crosstab3.to_excel(writer, sheet_name='Gerais Agosto2021', startcol=13 ,startrow= 72, index_label= 'cod_pac_final/UltDispVida')
    cd4_crosstab5.to_excel(writer, sheet_name='Gerais Agosto2021', startcol=3 ,startrow= 3, index_label='cod_pac_final/CD4maispertoPriDisp_ano')
    cd4_crosstab.to_excel(writer, sheet_name='Gerais Agosto2021', startcol=3 ,startrow= 12, index_label= 'anocoleta/UltCD4Ano')
    cd4_crosstab3.to_excel(writer, sheet_name='Gerais Agosto2021', startcol=8 ,startrow= 12, index_label= 'anocoleta/priCD4_antesTARV')
    cd4_crosstab2.to_excel(writer, sheet_name='Gerais Agosto2021', startcol=13 ,startrow= 12, index_label= 'anocoleta/PriCD4')
    cv_crosstab.to_excel(writer, sheet_name='Gerais Agosto2021', startcol=3 ,startrow= 40, index_label= 'anocoleta/UltCVAno')
    cv_crosstab2.to_excel(writer, sheet_name='Gerais Agosto2021', startcol=8 ,startrow= 40, index_label= 'anocoleta/PriCV')


# # Crosstabs - arquivo: 2 Indicadores TARV - sheet: (1) PriDisp&TARV  - Crosstabs CD4 - tabela: tb_dispensas_esquemas_udm.txt

# In[159]:


tb_dispensas_esquemas_udm.info()


# ## Crosstabs: dtdisp_mes * PriDisp * anodisp

# In[160]:


dispensa2_crosstab =  tb_dispensas_esquemas_udm['anodisp','dtdisp_mes','PriDisp']


# In[161]:


dispensa2_crosstab


# In[162]:


dispensa2_crosstab = dispensa2_crosstab.to_pandas_df(['anodisp','dtdisp_mes','PriDisp'])


# In[163]:


dispensa2_crosstab = dispensa2_crosstab.groupby(['anodisp','dtdisp_mes','PriDisp']).size().reset_index(name='Total')


# In[164]:


dispensa2_crosstab = pd.pivot_table(dispensa2_crosstab, values='Total', index=['anodisp', 'dtdisp_mes'],
                    columns=[ 'PriDisp'], aggfunc=np.sum)


# In[165]:


dispensa2_crosstab


# ## Soma das colunas da tabela dispensa2_crosstab

# In[166]:


dispensa2_crosstab['Total'] = dispensa2_crosstab.loc[:, 0:].apply(np.sum, axis=1)


# In[167]:


total = dispensa2_crosstab.sum()
total.name = 'Total'
dispensa2_crosstab = dispensa2_crosstab.append(total.transpose())


# ## Crosstabs: dtdisp_mes * UltDispAno * anodisp

# In[168]:


tb_dispensas_esquemas_udm.info()


# In[169]:


dispensa2_crosstab2 =  tb_dispensas_esquemas_udm['anodisp','dtdisp_mes','UltDispAno']


# In[170]:


dispensa2_crosstab2 = dispensa2_crosstab2.to_pandas_df(['anodisp','dtdisp_mes','UltDispAno'])


# In[171]:


dispensa2_crosstab2 = dispensa2_crosstab2.groupby(['anodisp','dtdisp_mes','UltDispAno']).size().reset_index(name='Total')


# In[172]:


dispensa2_crosstab2 = pd.pivot_table(dispensa2_crosstab2, values='Total', index=['anodisp', 'dtdisp_mes'],
                    columns=[ 'UltDispAno'], aggfunc=np.sum)


# In[173]:


dispensa2_crosstab2['Total'] = dispensa2_crosstab2.loc[:, 0:].apply(np.sum, axis=1)


# In[174]:


total = dispensa2_crosstab2.sum()
total.name = 'Total'
dispensa2_crosstab2 = dispensa2_crosstab2.append(total.transpose())


# In[175]:


dispensa2_crosstab2


# # Crosstabs : anodisp*PriDisp*UF / anodisp*UltDispAno*UF/ mes*PriDisp*UF

# ## Variável UF do banco de pacientes - arquivo: paciente_mar2021_unico.txt

# In[181]:


paciente_mar2021_unico = pd.read_csv('./paciente_mar2021_unico.txt', 
                       sep='\t',
                       error_bad_lines=False)   
                       


# In[183]:


paciente_mar2021_unico.info()


# In[186]:


UF_paciente =  paciente_mar2021_unico[['cod_pac_final', 'UF']]


# In[187]:


UF_paciente


# In[190]:


tb_dispensas_esquemas_udm.info()


# In[231]:


dispensa_pridisp_cod_pac = tb_dispensas_esquemas_udm[['codigo_paciente', 'PriDisp', 'UltDispAno', 'dtdisp_mes', 'anodisp']]


# In[232]:


dispensa_pridisp_cod_pac = dispensa_pridisp_cod_pac.to_pandas_df(['codigo_paciente', 'PriDisp', 'UltDispAno', 'dtdisp_mes', 'anodisp'])


# In[233]:


dispensa_pridisp_cod_pac = dispensa_pridisp_cod_pac.rename(columns={"codigo_paciente": "cod_pac_final"})


# In[234]:


tb_dispensas_esquemas_udm_paciente_uf_merged = pd.merge(dispensa_pridisp_cod_pac, UF_paciente)


# In[235]:


tb_dispensas_esquemas_udm_paciente_uf_merged


# ## anodisp*PriDisp*UF

# In[215]:


cross_dispensa_paciente_merged = tb_dispensas_esquemas_udm_paciente_uf_merged.groupby([ 'UF','anodisp', 'PriDisp' ]).size().reset_index(name='Total')


# In[216]:


cross_dispensa_paciente_merged = pd.pivot_table(cross_dispensa_paciente_merged, values='Total', index=['UF', 'PriDisp'],
                    columns=[ 'anodisp'], aggfunc=np.sum)


# In[224]:


cross_dispensa_paciente_merged


# In[221]:


cross_dispensa_paciente_merged['Total'] = cross_dispensa_paciente_merged.loc[:, :].apply(np.sum, axis=1)


# In[223]:


total = cross_dispensa_paciente_merged.sum()
total.name = 'Total'
cross_dispensa_paciente_merged = cross_dispensa_paciente_merged.append(total.transpose())


# ## anodisp*UltDispAno*UF

# In[225]:


cross_dispensa_paciente_merged2 = tb_dispensas_esquemas_udm_paciente_uf_merged.groupby([ 'UF','anodisp', 'UltDispAno' ]).size().reset_index(name='Total')


# In[226]:


cross_dispensa_paciente_merged2 = pd.pivot_table(cross_dispensa_paciente_merged2, values='Total', index=['UF', 'UltDispAno'],
                    columns=[ 'anodisp'], aggfunc=np.sum)


# In[230]:


cross_dispensa_paciente_merged2


# In[228]:


cross_dispensa_paciente_merged2['Total'] = cross_dispensa_paciente_merged2.loc[:, :].apply(np.sum, axis=1)


# In[229]:


total = cross_dispensa_paciente_merged2.sum()
total.name = 'Total'
cross_dispensa_paciente_merged2 = cross_dispensa_paciente_merged2.append(total.transpose())


# ## dtdisp_mes*PriDisp*UF

# In[236]:


cross_dispensa_paciente_merged3 = tb_dispensas_esquemas_udm_paciente_uf_merged.groupby([ 'UF','dtdisp_mes' ]).size().reset_index(name='Total')


# In[237]:


cross_dispensa_paciente_merged3 = pd.pivot_table(cross_dispensa_paciente_merged3, values='Total', index=['UF'],
                    columns=[ 'dtdisp_mes'], aggfunc=np.sum)


# In[241]:


cross_dispensa_paciente_merged3


# In[240]:


cross_dispensa_paciente_merged3['Total'] = cross_dispensa_paciente_merged3.loc[:,:].apply(np.sum, axis=1)


# In[242]:


total = cross_dispensa_paciente_merged3.sum()
total.name = 'Total'
cross_dispensa_paciente_merged3 = cross_dispensa_paciente_merged3.append(total.transpose())


# In[243]:


cross_dispensa_paciente_merged3


# ## Salvar Arquivo: 2 Indicadores TARV - Sheet: (1) PriDisp&TARV

# In[244]:


with pd.ExcelWriter('2 Indicadores Agosto2021_TARV.xlsx') as writer:  
    dispensa2_crosstab.to_excel(writer, sheet_name='(1) PriDisp&TARV Agosto2021', startcol=3 ,startrow= 3, index_label= 'anodisp/dtdisp_mes/PriDisp')
    table_crosstab2.to_excel(writer, sheet_name='(1) PriDisp&TARV Agosto2021', startcol=8 ,startrow= 3, index_label= 'anodisp/PriDisp')
    dispensa2_crosstab2.to_excel(writer, sheet_name='(1a) UltDispAnoMes Agosto2021', startcol=3 ,startrow= 3, index_label= 'anodisp/dtdisp_mes/UltDispAno')
    cross_dispensa_paciente_merged.to_excel(writer, sheet_name='(2) PriDisp UF Agosto2021', startcol=3 ,startrow= 3, index_label= 'UF/PriDisp/anodisp')
    cross_dispensa_paciente_merged2.to_excel(writer, sheet_name='(3) UltDispAno UF Agosto2021', startcol=3 ,startrow= 3, index_label= 'UF/UltDispAno/anodisp')
    cross_dispensa_paciente_merged3.to_excel(writer, sheet_name='(4) PriDisp Mês UF Agosto2021', startcol=3 ,startrow= 3, index_label= 'UF/PriDisp/dtmes_disp')

