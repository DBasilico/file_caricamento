import pyspark.sql.functions as f


class run_par(dict):
    def __init__(self, spark):
        configurazione = spark.table('data.rfcf_storico_run')\
                              .filter(f.col('flag_run_corrente')=='Y').collect()
        if len(configurazione) != 1:
            print(configurazione)
            raise Exception('ERRORE: ci sono righe multiple in configurazione start run con flag Y')
        else:
            par = configurazione[0].asDict()
            for c in par:
                self[c] = par[c]

    def __getattr__(self, name):
        return self[name]


class run_par_cashflow(dict):
    def __init__(self, spark):
        configurazione = spark.table('lab1_db.rfcf_configurazione_start_run_cashflow').collect()[0].asDict()
        if configurazione['puntamento_id_run']>0 and configurazione['vista_forecast_cashflow']=='default':
          raise Exception('ERRORE: "puntamento_id_run">0 e "vista_forecast_cashflow"==default')

        if configurazione['puntamento_id_run']<0:
          par = spark.table('lab1_db.rfcf_storico_run')\
                     .filter(f.col('flg_run_corrente')=='Y').collect()[0].asDict()
        else:
          par = spark.table('lab1_db.rfcf_storico_run')\
                     .filter(f.col('id_run')==configurazione['puntamento_id_run']).collect()[0].asDict()

        par_cf = spark.table('lab1_db.rfcf_storico_run_cashflow')\
                      .filter(f.col('flg_run_corrente_cashflow') == 'Y').collect()[0].asDict()
        for c in par:
            self[c] = par[c]
        for c in par_cf:
            self[c] = par_cf[c]

    def __getattr__(self, name):
        return self[name]

