import os
import datetime
import pytz
import pyspark.sql.functions as f
from pyspark.sql.types import *
from copy import deepcopy
from delta.tables import DeltaTable


def file_ultima_modifica(dirToCheck, lista_di_file=None):
    if not os.path.isdir(dirToCheck):
        raise Exception(f'Il percorso \n {dirToCheck} \n non e` valido')
    ret_file = None
    max_ts = datetime.datetime.min
    if lista_di_file is not None and len(lista_di_file)>0:
        for f in lista_di_file:
            fileToCheck = os.path.join(dirToCheck,f)
            ts = datetime.datetime.fromtimestamp(os.path.getmtime(fileToCheck))
            print(f'{f} --> {ts}')
            if ts>max_ts:
                ret_file = f
    else:
        for base, _, file in os.walk(dirToCheck):
            for f in file:
                fileToCheck = os.path.join(dirToCheck,f)
                ts = datetime.datetime.fromtimestamp(os.path.getmtime(fileToCheck))
                print(f'{f} --> {ts}')
                if ts>max_ts:
                    ret_file = f
    return ret_file

  
def last_update_folder(dirToCheck, lista_di_file=None):
    # lista di file senza percorso cartella base
    tss = list()
    if lista_di_file is not None and len(lista_di_file)>0:
        for f in lista_di_file:
            fileToCheck = os.path.join(dirToCheck,f)
            ts = os.path.getmtime(fileToCheck)
            tss.append(ts)
    else:
        for base, _, file in os.walk(dirToCheck):
            for f in file:
                fileToCheck = os.path.join(base, f)
                ts = os.path.getmtime(fileToCheck)
                tss.append(ts)
    dt = datetime.datetime.fromtimestamp(max(tss))
    return datetime.datetime(year=dt.year, month=dt.month, day=dt.day, 
                             hour=dt.hour, minute=dt.minute, second=dt.second)


def utc_to_local(utc_dt):
    local_tz = pytz.timezone('Europe/Rome')
    local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz)
    return local_tz.normalize(local_dt)


def storicizza_input(spark, output_table, base_path, schema_tabella, crea_tabella, updCheckFiles=None, **crea_tabella_args):
    campiStoricizzazione = ['files_last_modify', 'files_last_modify_UTC', 'data_elaborazione', 'flag_ultimo_record_valido']
    if not spark.catalog._jcatalog.tableExists(output_table):
        print('Tabella non trovata --> CREATA TABELLA')
        builder_schema = deepcopy(schema_tabella)
        for name in campiStoricizzazione:
            builder_schema.add(StructField(name, StringType(), False))
        spark.createDataFrame(spark.sparkContext.emptyRDD(), schema=builder_schema) \
            .write.mode('overwrite').format('delta').saveAsTable(output_table)

    df = spark.table(output_table).cache()
    last_modify_files = last_update_folder(dirToCheck=base_path,
                                           lista_di_file=updCheckFiles)

    if df.count() > 0:
        if df.filter(f.col(campiStoricizzazione[3]) == 'Y').select(campiStoricizzazione[1]).distinct().count() == 1:
            last_modify_table = df.filter(f.col(campiStoricizzazione[3]) == 'Y').select(campiStoricizzazione[1]).collect()[0].files_last_modify_UTC
            last_modify_table = datetime.datetime.strptime(last_modify_table, '%Y-%m-%d %H:%M:%S')
        else:
            raise Exception(f'{output_table}: files_last_modify_UTC multipli o nulli per flag_ultimo_record_valido == Y')

        if last_modify_files > last_modify_table:
            print('Rilevate modifiche ai files di input --> APPESA TABELLA')
            print(f'Ultima data modifica file input in tabella: {last_modify_table}')
            print(f'Ultima modifica files: {last_modify_files}')
            DeltaTable.forName(spark, output_table).update(condition=f"{campiStoricizzazione[3]}=='Y'",
                                                           set={f'{campiStoricizzazione[3]}': "'N'"})
            df_tab_creata = crea_tabella(spark, schema_tabella=schema_tabella, base_path=base_path, **crea_tabella_args)
            df_tab_creata = df_tab_creata.withColumn(campiStoricizzazione[0], f.lit(utc_to_local(last_modify_files).strftime('%Y-%m-%d %H:%M:%S')))
            df_tab_creata = df_tab_creata.withColumn(campiStoricizzazione[1], f.lit(last_modify_files.strftime('%Y-%m-%d %H:%M:%S')))
            df_tab_creata = df_tab_creata.withColumn(campiStoricizzazione[2], f.lit(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            df_tab_creata = df_tab_creata.withColumn(campiStoricizzazione[3], f.lit('Y'))
            df_tab_creata.write.mode('append').format('delta').saveAsTable(output_table)
        else:
            print('Nessuna modifica rilevata ai file di input')
            print(f'Ultima data modifica file input in tabella: {last_modify_table}')
            print(f'Ultima modifica files: {last_modify_files}')
    else:
        print('Tabella vuolta --> APPESA TABELLA')
        df_tab_creata = crea_tabella(spark, schema_tabella=schema_tabella, base_path=base_path, **crea_tabella_args)
        df_tab_creata = df_tab_creata.withColumn(campiStoricizzazione[0], f.lit(utc_to_local(last_modify_files).strftime('%Y-%m-%d %H:%M:%S')))
        df_tab_creata = df_tab_creata.withColumn(campiStoricizzazione[1], f.lit(last_modify_files.strftime('%Y-%m-%d %H:%M:%S')))
        df_tab_creata = df_tab_creata.withColumn(campiStoricizzazione[2], f.lit(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        df_tab_creata = df_tab_creata.withColumn(campiStoricizzazione[3], f.lit('Y'))
        df_tab_creata.write.mode('append').format('delta').saveAsTable(output_table)


def controlli(spark, df_new_name, df_old_name):
    df_old = spark.table(df_old_name)
    df_new = spark.table(df_new_name)
    col_to_check = df_old.columns.copy()
    
    df_old = df_old.select(*[f.col(x).alias(f'{x.upper()}_OLD') for x in df_old.columns])
    df_new = df_new.select(*[f.col(x).alias(f'{x.upper()}_NEW') for x in df_new.columns])
    
    conds = list()
    for c in col_to_check:
        conds.append(  (df_new[f'{c}_NEW'] == df_old[f'{c}_OLD']) | ((df_new[f'{c}_NEW'].isNull())&( df_old[f'{c}_OLD'].isNull()))      )
    
    col_controllo = 'tipo'
    
    conds = ((f.col(f'{col_controllo}_NEW').isNull()) & ~(f.col(f'{col_controllo}_OLD').isNull())) | (~(f.col(f'{col_controllo}_NEW').isNull()) & (f.col(f'{col_controllo}_OLD').isNull()))
    
    df_check = df_new.join(df_old, on=conds, how='full').filter(conds)
    
    print(f'Righe non allineate: {df_check.count()}')
    df_check.display()