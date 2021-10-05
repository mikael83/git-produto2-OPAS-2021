#!/usr/bin/env python
# coding: utf-8

# # Script Python - Jupyter - BANCO DE DISPENSA - CONVERSÃO - TXT PARA HDF5 
# # 20/09/21 por Mikael Lemos - V1.0 

# ## Carregando as bibliotecas

# In[1]:


import os
import openpyxl
import black 
import numpy as np
import pandas as pd
import pyreadstat
from datetime import datetime, date, time, timezone
import pyflowchart
import vaex
from pathlib import Path
import h5py


# ## Carregando dados/indicar pasta

# In[2]:


#os.chdir("C:/Users/lemos/Downloads/AMA/produtos_opas/2021/produto2/dispensa")


# ## Ler arquivos TXT como CSV - Dataframe

# In[4]:


tb_dispensas_esquemas_udm = pd.read_csv('/mnt/c/Users/lemos/Downloads/AMA/produtos_opas/2021/produto2/dispensa/tb_dispensas_esquemas_udm.txt', 
                       sep='\t',
                       error_bad_lines=False, 
                       header=None)    


# ## Incluir nomes das colunas 

# In[5]:


tb_dispensas_esquemas_udm = tb_dispensas_esquemas_udm.set_axis(["codigo_paciente","cod_pac_final", "codigo_udm","num_solicit","cod_ibge_udm","categoria_disp","st_profilaxia","idade","categoria_crianca","categoria_usuario","periodo_gest","validade_form","peso_kg","st_tb","st_hbv","st_hcv","motivo_mudanca_tratamento","st_mutacao_drv","st_falha_3tc","medicamento_mud_1","medicamento_mud_2","medicamento_mud_3","medicamento_mud_4","medicamento_mud_5","just_outros","st_arv_restrito","st_situacao_especial","nu_protocolo","ultima_carga_viral_digitada_siclom","data_exame_cv","st_exame_cv","data_exame_cd4","ultimo_cd4_digitado_siclom","perc_cd4","st_exame_cd4","st_dtg","st_ajuste_tdf","st_atz","st_esquema_dtg_dobro","data_desfecho_gestacao","data_dispensa","esquema","esquema_forma","duracao","cd_crm","uf_crm","data_PriDisp","PriDisp","UltDisp_2009","UltDisp_2010","UltDisp_2011","UltDisp_2012","UltDisp_2013","UltDisp_2014","UltDisp_2015","UltDisp_2016","UltDisp_2017","UltDisp_2018","UltDisp_2019","UltDisp_2020","UltDisp_2021","UltDispVida","data_dispensa_anterior","data_dispensa_posterior","data_ultima_dispensa","duracao_dispensa_anterior","duracao_dispensa_posterior"]  , axis=1)


# In[6]:


#names=["codigo_paciente","cod_pac_final", "codigo_udm","num_solicit","cod_ibge_udm","categoria_disp","st_profilaxia","idade","categoria_crianca","categoria_usuario","periodo_gest","validade_form","peso_kg","st_tb","st_hbv","st_hcv","motivo_mudanca_tratamento","st_mutacao_drv","st_falha_3tc","medicamento_mud_1","medicamento_mud_2","medicamento_mud_3","medicamento_mud_4","medicamento_mud_5","just_outros","st_arv_restrito","st_situacao_especial","nu_protocolo","ultima_carga_viral_digitada_siclom","data_exame_cv","st_exame_cv","data_exame_cd4","ultimo_cd4_digitado_siclom","perc_cd4","st_exame_cd4","st_dtg","st_ajuste_tdf","st_atz","st_esquema_dtg_dobro","data_desfecho_gestacao","data_dispensa","esquema","esquema_forma","duracao","cd_crm","uf_crm","data_PriDisp","PriDisp","UltDisp_2009","UltDisp_2010","UltDisp_2011","UltDisp_2012","UltDisp_2013","UltDisp_2014","UltDisp_2015","UltDisp_2016","UltDisp_2017","UltDisp_2018","UltDisp_2019","UltDisp_2020","UltDisp_2021","UltDispVida","data_dispensa_anterior","data_dispensa_posterior","data_ultima_dispensa","duracao_dispensa_anterior","duracao_dispensa_posterior"]  


# ## Conferir tabela

# In[7]:


tb_dispensas_esquemas_udm.head()


# ## Verificar uso de memória - passo opcional

# In[8]:


tb_dispensas_esquemas_udm.info(memory_usage='deep')


# ## Salvar como arquivo hdf5 (formato binário)

# In[9]:


vaex_tb_dispensas_esquemas_udm = vaex.from_pandas(tb_dispensas_esquemas_udm, copy_index=False)


# In[10]:


vaex_tb_dispensas_esquemas_udm.export('/mnt/c/Users/lemos/Downloads/AMA/produtos_opas/2021/produto2/dispensa/tb_dispensas_esquemas_udm.hdf5')


# ## Ler arquivos TXT como CSV - Dataframe - CD4 - tb_cd4_consolidado.txt

# In[2]:


tb_cd4_consolidado = pd.read_csv('/mnt/c/Users/lemos/Downloads/AMA/produtos_opas/2021/produto2/dispensa/tb_cd4_consolidado.txt', 
                       sep='\t',
                       error_bad_lines=False, 
                       header=None)  


# ## Incluir nome das colunas

# In[3]:


names2=["cod_pac","cod_pac_final", "cod_ibge_udm","paciente_gestante","motivo_exame","cd_ibge_coletora","cd_ibge_executora","data_solicitacao","data_hora_coleta","data_do_resultado","condicoes_chegada","contagem_cd4","perc_cd4","contagem_cd8","perc_cd8","sintomatico","dt_primeira_dispensa","dt_ultimo_exame","HIV","PriCD4","UltCD4_2009","UltCD4_2010","UltCD4_2011","UltCD4_2012","UltCD4_2013","UltCD4_2014","UltCD4_2015","UltCD4_2016","UltCD4_2017","UltCD4_2018","UltCD4_2019","UltCD4_2020","UltCD4_2021","UltCD4_antesTARV","tipo_unidade"]  


# In[4]:


tb_cd4_consolidado = tb_cd4_consolidado.set_axis([names2]  , axis=1)


# In[5]:


tb_cd4_consolidado.info()


# In[6]:


tb_cd4_consolidado['UltCD4_2009'] = tb_cd4_consolidado['UltCD4_2009'].fillna(0).astype(np.int64)
tb_cd4_consolidado['UltCD4_2010'] = tb_cd4_consolidado['UltCD4_2010'].fillna(0).astype(np.int64)
tb_cd4_consolidado['UltCD4_2011'] = tb_cd4_consolidado['UltCD4_2011'].fillna(0).astype(np.int64)
tb_cd4_consolidado['UltCD4_2012'] = tb_cd4_consolidado['UltCD4_2012'].fillna(0).astype(np.int64)
tb_cd4_consolidado['UltCD4_2013'] = tb_cd4_consolidado['UltCD4_2013'].fillna(0).astype(np.int64)
tb_cd4_consolidado['UltCD4_2014'] = tb_cd4_consolidado['UltCD4_2014'].fillna(0).astype(np.int64)
tb_cd4_consolidado['UltCD4_2015'] = tb_cd4_consolidado['UltCD4_2015'].fillna(0).astype(np.int64)
tb_cd4_consolidado['UltCD4_2016'] = tb_cd4_consolidado['UltCD4_2016'].fillna(0).astype(np.int64)
tb_cd4_consolidado['UltCD4_2017'] = tb_cd4_consolidado['UltCD4_2017'].fillna(0).astype(np.int64)
tb_cd4_consolidado['UltCD4_2018'] = tb_cd4_consolidado['UltCD4_2018'].fillna(0).astype(np.int64)
tb_cd4_consolidado['UltCD4_2019'] = tb_cd4_consolidado['UltCD4_2019'].fillna(0).astype(np.int64)
tb_cd4_consolidado['UltCD4_2020'] = tb_cd4_consolidado['UltCD4_2020'].fillna(0).astype(np.int64)
tb_cd4_consolidado['UltCD4_2021'] = tb_cd4_consolidado['UltCD4_2021'].fillna(0).astype(np.int64)


# In[8]:


tb_cd4_consolidado


# ## Salvar como arquivo hdf5 (formato binário)

# In[9]:


vaex_tb_cd4_consolidado = vaex.from_pandas(tb_cd4_consolidado, copy_index=True)


# In[ ]:


vaex_tb_cd4_consolidado.export('/mnt/c/Users/lemos/Downloads/AMA/produtos_opas/2021/produto2/dispensa/tb_cd4_consolidado.hdf5')


# ## Ler arquivos TXT como CSV - Dataframe - CD4 - tb_carga_viral_consolidado.txt

# In[2]:


tb_carga_viral_consolidado = pd.read_csv('/mnt/c/Users/lemos/Downloads/AMA/produtos_opas/2021/produto2/dispensa/tb_carga_viral_consolidado.txt', 
                       sep='\t',
                       error_bad_lines=False, 
                       header=None)  


# ## Incluir nome nas colunas 

# In[4]:


names3=["cod_pac","cod_pac_final", "cod_ibge_udm","paciente_gestante","motivo_exame","cd_ibge_coletora","cd_ibge_executora","data_solicitacao","data_hora_coleta","data_do_resultado","condicoes_chegada","contagem_cd4","perc_cd4","contagem_cd8","perc_cd8","sintomatico","dt_primeira_dispensa","dt_ultimo_exame","HIV","PriCV","UltCV_2009","UltCV_2010","UltCV_2011","UltCV_2012","UltCV_2013","UltCV_2014","UltCV_2015","UltCV_2016","UltCV_2017","UltCV_2018","UltCV_2019","UltCV_2020","UltCV_2021","UltCD4_antesTARV","tipo_unidade"]  


# In[5]:


tb_carga_viral_consolidado = tb_carga_viral_consolidado.set_axis([names3]  , axis=1)


# In[6]:


pd.set_option('display.max_columns', None)
tb_carga_viral_consolidado.head()


# In[7]:


tb_carga_viral_consolidado.info()


# In[8]:


tb_carga_viral_consolidado['UltCV_2009'] = tb_carga_viral_consolidado['UltCV_2009'].fillna(0).astype(np.int64)
tb_carga_viral_consolidado['UltCV_2010'] = tb_carga_viral_consolidado['UltCV_2010'].fillna(0).astype(np.int64)
tb_carga_viral_consolidado['UltCV_2011'] = tb_carga_viral_consolidado['UltCV_2011'].fillna(0).astype(np.int64)
tb_carga_viral_consolidado['UltCV_2012'] = tb_carga_viral_consolidado['UltCV_2012'].fillna(0).astype(np.int64)
tb_carga_viral_consolidado['UltCV_2013'] = tb_carga_viral_consolidado['UltCV_2013'].fillna(0).astype(np.int64)
tb_carga_viral_consolidado['UltCV_2014'] = tb_carga_viral_consolidado['UltCV_2014'].fillna(0).astype(np.int64)
tb_carga_viral_consolidado['UltCV_2015'] = tb_carga_viral_consolidado['UltCV_2015'].fillna(0).astype(np.int64)
tb_carga_viral_consolidado['UltCV_2016'] = tb_carga_viral_consolidado['UltCV_2016'].fillna(0).astype(np.int64)
tb_carga_viral_consolidado['UltCV_2017'] = tb_carga_viral_consolidado['UltCV_2017'].fillna(0).astype(np.int64)
tb_carga_viral_consolidado['UltCV_2018'] = tb_carga_viral_consolidado['UltCV_2018'].fillna(0).astype(np.int64)
tb_carga_viral_consolidado['UltCV_2019'] = tb_carga_viral_consolidado['UltCV_2019'].fillna(0).astype(np.int64)
tb_carga_viral_consolidado['UltCV_2020'] = tb_carga_viral_consolidado['UltCV_2020'].fillna(0).astype(np.int64)
tb_carga_viral_consolidado['UltCV_2021'] = tb_carga_viral_consolidado['UltCV_2021'].fillna(0).astype(np.int64)


# ## Salvar como arquivo hdf5 (formato binário)

# In[9]:


tb_carga_viral_consolidado


# In[10]:


vaex_tb_carga_viral_consolidado = vaex.from_pandas(tb_carga_viral_consolidado, copy_index=True)


# In[11]:


vaex_tb_carga_viral_consolidado.export('/mnt/c/Users/lemos/Downloads/AMA/produtos_opas/2021/produto2/dispensa/tb_carga_viral_consolidado.hdf5')

