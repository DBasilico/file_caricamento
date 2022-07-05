truncate table lab1_db.rfcf_configurazione_start_run;

-- se calcoliamo un anno solo forecast: mettere mese_fine_cons e max_emissione_cons a 0
-- se non vogliamo calcolare rateo anni precedenti, inserire 00000000 sotto ds_ric_gas_anno_prec e ds_ric_pwr_anno_prec
-- anno_prec_tipologia = DELTA o anno_prec_tipologia = EMISSIONE -> modalità di calcolo del rateo anni precedenti (utile nel BUDGET fatto a ottobre/novembre sull'anno successivo) 
	
insert into lab1_db.rfcf_configurazione_start_run (id_run_fw, anno_inizio, anno_fine, id_run_cons, mese_fine_cons, max_emissione_cons,ds_ric_gas_anno_prec,ds_ric_pwr_anno_prec,nota, anno_prec_tipologia) values('26','2022','2022','117','5','5','20220224','20220224','II Forecast','EMISSIONE');