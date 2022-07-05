truncate table lab1_db.rfcf_configurazione_start_run_cashflow;

-- se facciamo un run FCF2 che si aggangia all'ultimo run FCF1 mettiamo (-1,'default',-1,-1,-1,-1,'default')
-- altrimenti: per puntamento_id_run l'id_run FCF1 a cui vogliamo puntare e per vista_forecast_cashflow il nome del backup della vista previsiva FCF1

insert into lab1_db.rfcf_configurazione_start_run_cashflow (PUNTAMENTO_ID_RUN, VISTA_FORECAST_CASHFLOW,PUNTAMENTO_ANNO_INIZIO,PUNTAMENTO_ANNO_FINE,PUNTAMENTO_MESE_FINE_CONS,PUNTAMENTO_MAX_EMISSIONE_CONS,PUNTAMENTO_NOTA) values (-1,'default',-1,-1,-1,-1,'default');