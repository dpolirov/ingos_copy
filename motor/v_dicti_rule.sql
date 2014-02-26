CREATE OR REPLACE VIEW motor.v_dicti_rule AS 
 SELECT isn,parentisn,code,shortname,fullname,constname,active
   FROM ais.dicti
  WHERE shared_system.is_subtree(__hier, 683209116);