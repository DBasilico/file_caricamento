import pyspark.sql.functions as f
from pyspark.sql.types import *
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta


def calcolo_snapshot(spark,df_tabelle, ds_estremo_sup, id_run, id_run_fw, tab_snapshot_name, schema):
    df = list()
    if spark.table(tab_snapshot_name).filter( (f.col('id_run')==id_run)&(f.col('id_run_fw')==id_run_fw) ).count()>0:
        raise Exception(f'ERRORE: sono gia` presenti in snapshot id_run={id_run} e id_run_fw={id_run_fw}')
    print(f'Snapshot filtrato per {ds_estremo_sup}' if not pd.isna(ds_estremo_sup) else f'Nessun filtro a priori su snapshot')
    for idx in df_tabelle[df_tabelle.tipo_storicizzazione == 'snapshot'].index:
        snap_col_format = df_tabelle.loc[idx, 'tipo_dato']
        snap_col_name = df_tabelle.loc[idx, 'campo_snapshot']
        tab_name = df_tabelle.loc[idx, 'nome_tabella_completo']
        print(f'Creo snapshot per: {tab_name}')
        if snap_col_format != 'timestamp':
            raise Exception(f'{snap_col_format} formato non ancora gestito')
        
        if pd.isna(ds_estremo_sup):
            df_tab = spark.table(tab_name)
        else:
            df_tab = spark.table(tab_name).filter(f.col(snap_col_name)<ds_estremo_sup)
        max_ds = df_tab.groupBy()\
                       .agg(f.max(snap_col_name)).collect()[0][f"max({snap_col_name})"]
        
        if not pd.isna(max_ds):
            df.append({'id_run':id_run, 'id_run_fw':id_run_fw,
                       'nome_tabella':tab_name,
                       'data_snapshot':max_ds})
    
    spark.createDataFrame(df, schema=schema)\
         .write.mode('append').format('delta').saveAsTable(tab_snapshot_name)


def storicizzazione(spark,df_tabelle, data_inizio_run):
    for idx in df_tabelle[df_tabelle.tipo_storicizzazione == 'no_storico'].index:
        tab_name = df_tabelle.loc[idx, 'nome_tabella_completo']
        storico_tab_name = df_tabelle.loc[idx, 'nome_tabella_storico'] 
        print(f'Storicizzo: {tab_name} in {storico_tab_name} --> {data_inizio_run}')
        df_tab = spark.table(tab_name).withColumn('data_storico', f.lit(data_inizio_run))
        if not spark.catalog._jcatalog.tableExists(storico_tab_name):
            spark.createDataFrame(spark.sparkContext.emptyRDD(), schema=df_tab.schema)
        df_tab.write.mode('append').format('delta').saveAsTable(storico_tab_name)

        
def elimina_storicizzazione(spark, flusso):
    df_tabelle = spark.table('data.tcr_configurazione_storico_snapshot')\
                  .filter(f.col('flag_ultimo_record_valido')=='Y')\
                  .filter(f.col('flusso').like(f'%{flusso}%')).toPandas()
    df_tabelle['nome_tabella_completo'] = df_tabelle['database']+'.'+df_tabelle['nome_tabella']
    for idx in df_tabelle[df_tabelle.tipo_storicizzazione == 'no_storico'].index:
        tab_name = df_tabelle.loc[idx, 'nome_tabella_completo']
        storico_tab_name = df_tabelle.loc[idx, 'nome_tabella_storico']
        spark.sql(f'DROP TABLE IF EXISTS {storico_tab_name}')
        print(f'Tabella {storico_tab_name} eliminata')
    

def snapshot(spark, flusso, tab_snapshot_name, 
             id_run, id_run_fw, 
             data_forzatura_storico,
             data_inizio_run):
    file_path = '/Workspace/Repos/davide.basilico@external.eniplenitude.com/file_caricamento/CONDIVISI/Configurazioni/Snapshot.xlsx'
    
    if flusso not in ['rfcf_fatturato','rfcf_cashflow','crv','tariffario_xe']:
        raise Exception('flusso: valore non valido')
    
    df_tabelle = spark.table('data.tcr_configurazione_storico_snapshot')\
                      .filter(f.col('flag_ultimo_record_valido')=='Y')\
                      .filter(f.col('flusso').like(f'%{flusso}%')).toPandas()
    df_tabelle['nome_tabella_completo'] = df_tabelle['database']+'.'+df_tabelle['nome_tabella']
    
    if len(set(df_tabelle['tipo_storicizzazione'])-set(['start_end','snapshot','no_storico']))>0:
        raise Exception(f"Il tipo {set(df_tabelle['tipo_storicizzazione'])-set(['start_end','snapshot','no_storico'])} non e` gestito")
    
    schema_finale = StructType([StructField('id_run',IntegerType(),False),
                            StructField('id_run_fw',IntegerType(),False),
                            StructField('nome_tabella',StringType(),False),
                            StructField('data_snapshot',TimestampType(),False)])
    
    if not spark.catalog._jcatalog.tableExists(tab_snapshot_name):
        spark.createDataFrame(spark.sparkContext.emptyRDD(), schema=schema_finale)\
             .write.format('delta').saveAsTable(tab_snapshot_name)
    
    if pd.isna(data_forzatura_storico) :
        calcolo_snapshot(spark, 
                         df_tabelle = df_tabelle, 
                         ds_estremo_sup = None,
                         id_run = id_run,
                         id_run_fw = id_run_fw,
                         tab_snapshot_name = tab_snapshot_name,
                         schema = schema_finale)
        storicizzazione(spark,
                        df_tabelle = df_tabelle, 
                        data_inizio_run = data_inizio_run)
    else:
        ds_estremo_sup = data_forzatura_storico
        calcolo_snapshot(spark,
                         df_tabelle = df_tabelle, 
                         ds_estremo_sup = ds_estremo_sup,
                         id_run = id_run,
                         id_run_fw = id_run_fw,
                         tab_snapshot_name = tab_snapshot_name,
                         schema = schema_finale)

        

      
                  
   
        
        