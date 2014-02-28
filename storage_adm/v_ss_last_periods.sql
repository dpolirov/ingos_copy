CREATE OR REPLACE VIEW storage_adm.v_ss_last_periods (   prm_key )
AS

               select cast(DT||Isn as numeric)  PRM_Key
               from  (
               Select  To_Char(oracompat.ADD_MONTHS(date_trunc('month',current_timestamp)::DATE,-generate_series+1),'YYYYMMDD') Dt
               from generate_series(1,12)
               )a , ais.vy_subacc_plan
               Where   oracompat.ADD_MONTHS(date_trunc('month',current_timestamp)::DATE,-12) <= Dateend
               