CREATE OR REPLACE PACKAGE "STORAGES"."REPORT_RNP_NEW" IS

    FUNCTION GetActiveLoad(pDateRep IN DATE) RETURN NUMBER;

    PROCEDURE make_rnp_all(pDateRep IN DATE := NULL);
    
    -- общие структуры данных
    PROCEDURE make_longagr_paysum(pLoadIsn IN NUMBER := NULL);  
   
  -- РНП по МСФО
  PROCEDURE make_rnp_msfo_all(pLoadIsn IN NUMBER := NULL);
  PROCEDURE make_rnp_msfo_by_isn(pLoadIsn IN number, pMinIsn IN NUMBER, pMaxIsn IN NUMBER);
  PROCEDURE make_rnp_msfo_r_by_isn(pLoadIsn IN NUMBER, pMinIsn IN NUMBER, pMaxIsn IN NUMBER);

  -- РНП по РСБУ
  PROCEDURE make_rnp_rsbu_all(pLoadIsn IN NUMBER := NULL);
  PROCEDURE make_rnp_rsbu_by_isn(pLoadIsn IN NUMBER, pMinIsn IN NUMBER, pMaxIsn IN NUMBER);
  PROCEDURE make_rnp_rsbu_r_by_isn(ploadisn IN NUMBER,pMinIsn IN NUMBER, pMaxIsn IN NUMBER);
  PROCEDURE make_resrnpsummary(ploadisn IN NUMBER); -- EGAO 14.06.2013

  -- Доля перестраховщиков
  PROCEDURE make_rnp_re_rsbu(pLoadIsn NUMBER := NULL);
  PROCEDURE make_rnp_re_rsbu_by_isn(pLoadIsn NUMBER, pMinIsn Number,pMaxIsn Number);

  PROCEDURE make_rnp_re_msfo(pLoadIsn NUMBER := NULL);
  PROCEDURE make_rnp_re_msfo_virtualcond(pLoadisn IN NUMBER);
  PROCEDURE make_rnp_re_msfo_vcond_by_agr(pLoadisn IN NUMBER, pMinIsn IN NUMBER, pMaxIsn IN NUMBER);
  PROCEDURE make_rnp_re_msfo_xl(pLoadIsn IN NUMBER);
  PROCEDURE make_rnp_re_msfo_xl_by_sect(pLoadisn IN NUMBER, pSectIsn IN NUMBER);
  PROCEDURE make_rnp_re_msfo_prem(pLoadIsn IN NUMBER);
  PROCEDURE make_rnp_re_msfo_prem_by_isn(pLoadIsn IN NUMBER, pMinIsn IN NUMBER, pMaxIsn IN NUMBER);
  PROCEDURE make_rnp_re_msfo_rnp(pLoadIsn IN NUMBER);
  PROCEDURE make_rnp_re_msfo_rnp_by_isn(pLoadIsn IN NUMBER, pMinIsn IN NUMBER, pMaxIsn IN NUMBER);
  PROCEDURE make_rnp_re_msfo_final(pLoadIsn IN NUMBER);
  PROCEDURE make_rnp_re_msfo_final_by_sect(pLoadIsn IN NUMBER, pMinIsn IN NUMBER, pMaxIsn IN NUMBER);

  --Витрина разбивки долей перестраховщиков в РНП на перестраховщиков
  PROCEDURE make_rnp_re_subject(pLoadIsn IN NUMBER := NULL);
  
  --Договоры ОСАГО с ограниченным периодом использования ТС
  PROCEDURE make_rnposagoshortagr(pLoadIsn IN NUMBER);
  
END;

CREATE OR REPLACE PACKAGE BODY "STORAGES"."REPORT_RNP_NEW" IS
  CargoDept      constant Number := c.get('CargoDept');
  PrivDept       constant Number := c.get('PrivDept');
  CreditDept     constant Number := c.get('CreditDept');
  AgrInFacult    constant Number := c.get('agrInFacult');
  AgrInRetroc    constant Number := c.get('agrInRetroc');
  AgrInOblig     constant Number := c.get('AgrInOblig');
  EuroCurr       constant Number := c.get ('EUR'); -- 29448516
  DollarCurr     constant Number := c.get ('USD'); -- 53
  LocalCurr      constant Number := c.get ('LocalCurr'); -- 35
  AgrCrgExhibition  constant Number := c.get ('agrcrgexhibition');
  AgrFacultType NUMBER := c.get('AGROUTFACULT');
  AgrObligType NUMBER := c.get('AGROUTOBLIG');
  ReSchemaBySection CONSTANT VARCHAR2(1):='S';
  ReSchemaByRole CONSTANT VARCHAR2(1):='R';
  CarrierDept  constant Number := c.get('CARRIERDEPT');  -- EGAO 30.08.2012 в рамках ДИТ-12-3-172875
  TirRuleAgr CONSTANT NUMBER := 655469916; -- EGAO 30.08.2012 в рамках ДИТ-12-3-172875
  CosmicRuleAgr CONSTANT NUMBER := 683213616; -- EGAO 14.03.2013

  ProcessIsn CONSTANT NUMBER := 57;

  en_invalid_loadisn CONSTANT NUMBER := -20001;
  en_invalid_daterep CONSTANT NUMBER := -20002;

  exc_invalid_loadisn EXCEPTION;
  exc_invalid_daterep EXCEPTION;

  PRAGMA EXCEPTION_INIT(exc_invalid_loadisn, -20001);
  PRAGMA EXCEPTION_INIT(exc_invalid_daterep, -20002);

  FUNCTION MakeActiveLoad(pDateRep IN DATE) RETURN NUMBER
  IS
    vLoadIsn NUMBER;
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    -- сформировали новую загрузку
    vLoadIsn := REPORT_STORAGE.createload(pProcType=>ProcessIsn,pDaterep=>pDaterep,pclassisn=>1);
    -- сделали загрузку активной
    UPDATE Repload a SET a.classisn = NULL
    WHERE a.procisn=ProcessIsn AND a.daterep=pDaterep AND a.isn<>vLoadIsn;
    COMMIT;
    RETURN vLoadIsn;
  END;

  FUNCTION GetProcIsn RETURN NUMBER
  IS
  BEGIN
    RETURN ProcessIsn;
  END;

  FUNCTION GetDateRep(pLoadIsn IN NUMBER) RETURN DATE
  IS
    vDateRep DATE;
  BEGIN
    SELECT max(a.daterep)
    INTO vDateRep
    FROM repload a
    WHERE a.isn=pLoadIsn;

    RETURN vDateRep;
  END;

  FUNCTION GetActiveLoad(pDateRep IN DATE) RETURN NUMBER
  IS
    vLoad NUMBER;
  BEGIN
    SELECT MIN(isn)
    INTO vLoad
    FROM repload a
    Where a.procisn=ProcessIsn AND a.Daterep=pDaterep AND a.classIsn=1;

    RETURN vLoad;
  END;

  PROCEDURE make_rnp_all(pDateRep IN DATE := NULL)
  IS
    vDateRep DATE := nvl(pDateRep,trunc(SYSDATE,'mm')-1);
    vLoadIsn NUMBER;
  BEGIN
    vLoadIsn := MakeActiveLoad(vDateRep);

    replog_i (vLoadIsn, 'LoadRNPNEW', 'make_longagr_paysum',pAction=>'Begin');
    make_longagr_paysum(vLoadIsn);
    replog_i (vLoadIsn, 'LoadRNPNEW', 'make_longagr_paysum',pAction=>'End');
    
    replog_i (vLoadIsn, 'LoadRNPNEW', 'make_rnposagoshortagr',pAction=>'Begin');
    make_rnposagoshortagr(vLoadIsn);
    replog_i (vLoadIsn, 'LoadRNPNEW', 'make_rnposagoshortagr',pAction=>'End');

    replog_i (vLoadIsn, 'LoadRNPNEW', 'make_rnp_msfo_all',pAction=>'Begin');
    make_rnp_msfo_all(vLoadIsn);
    replog_i (vLoadIsn, 'LoadRNPNEW', 'make_rnp_msfo_all',pAction=>'End');

    replog_i (vLoadIsn, 'LoadRNPNEW', 'make_rnp_rsbu_all',pAction=>'Begin');
    make_rnp_rsbu_all(vLoadIsn);
    replog_i (vLoadIsn, 'LoadRNPNEW', 'make_rnp_rsbu_all',pAction=>'End');

    replog_i (vLoadIsn, 'LoadRNPNEW', 'make_rnp_re_rsbu',pAction=>'Begin');
    make_rnp_re_rsbu(vLoadIsn);
    replog_i (vLoadIsn, 'LoadRNPNEW', 'make_rnp_re_rsbu',pAction=>'End');

    replog_i (vLoadIsn, 'LoadRNPNEW', 'make_rnp_re_msfo',pAction=>'Begin');
    make_rnp_re_msfo(vLoadIsn);
    replog_i (vLoadIsn, 'LoadRNPNEW', 'make_rnp_re_msfo',pAction=>'End');

    replog_i (vLoadIsn, 'LoadRNPNEW', 'make_rnp_re_subject',pAction=>'Begin');
    make_rnp_re_subject(vLoadIsn);
    replog_i (vLoadIsn, 'LoadRNPNEW', 'make_rnp_re_subject',pAction=>'End');

  END;

  PROCEDURE make_longagr_paysum(pLoadIsn IN NUMBER)
  IS
    vLoadIsn NUMBER := nvl(pLoadIsn, GetActiveLoad(trunc(SYSDATE,'mm')-1));
    vDateRep DATE := GetDateRep(vLoadIsn);
    LoadObjCnt  constant Number := 50000;
    vMinIsn     Number:=0;
    vMaxIsn     Number := 0;
    vCnt        Number:=0;
    sesid       Number;
    vSql        Varchar2(4000);
    vPart VARCHAR2(30);
  BEGIN
    IF vLoadIsn IS NULL THEN
      raise_application_error(en_invalid_loadisn,'Invalid loadisn');
    END IF;
    IF vDateRep IS NULL  THEN
      raise_application_error(en_invalid_daterep,'Invalid daterep');
    END IF;

    vPart:=init_partition_by_key(pTableName => 'storages.rep_longagr_paysum',pKey => vLoadIsn,pCompress => 1);

    -- для начислений до 01.01.202 остается вопрос разнесения на условия договора. сейчас добавляем начисления на договор
    insert into rep_longagr_paysum (isn, agrisn, dateval, paidsum, loadisn, agrcurrisn, agrsharepc, statcode, daterep, repclassisn)
    select Seq_Reports.NEXTVAL, agrisn, datepay, PaySum, vLoadIsn, currisn, sharepc, 38, vDateRep, 1
    from (select --+ use_nl (s a) index (a X_REPAGR_AGR) ordered
                 s.agrisn, s.datepay, a.currisn, nvl (a.sharepc,100)/100 sharepc,
                 sum (getcrosscover (s.amount,s.currisn,a.currisn,s.datepay)) PaySum
          from repdocsum s, repagr a
          where s.classisn = 414
            and s.discr = 'F'
            and s.agrisn = a.agrisn
            and months_between(dateend+1,datebeg)>13
          group by s.agrisn, s.datepay, a.currisn, a.sharepc
         );
    COMMIT;

    SesId:=Parallel_Tasks.createnewsession();
    vMinIsn:=0;
    loop
      vMaxIsn:=cut_Table('storage_source.repagr','Agrisn',vMinIsn,pRowCount => LoadObjCnt);
      if (vMaxIsn is null) then Exit; end if;
      vSql:='
            declare
              vMinIsn number:='||vMinIsn||';
              vMaxIsn number:='||vMaxIsn||';
              vLoadIsn number:='||vLoadIsn||';
              vDateRep    date := to_date('''||to_char(vDateRep,'dd.mm.yyyy')||''',''dd.mm.yyyy'');
              vCnt number :='||vCnt||';
            BEGIN
              SYS.DBMS_APPLICATION_INFO.Set_Module (''rep_agr_paysum'',''Thread: ''||to_char(vCnt));

              INSERT INTO storages.rep_longagr_paysum(isn, agrisn, dateval, paidsum, loadisn, agrcurrisn, agrsharepc, condisn, statcode, premiumsum, daterep, repclassisn)
              SELECT Seq_Reports.NEXTVAL,
                     a.*
              FROM (
                    SELECT a.agrisn,
                           a.dateval,
                           SUM(paidsum*100/a.agrsharepc) AS paidsum, -- !!! очень важно.
                                                                -- при расчете эффективного окончания условия длинного договора
                                                                -- мы вычисляем отношение Начисленная премия(НП)/Плановая премия(ПП).
                                                                -- В АИС ПП храниться без учета доли ИГС, а начисления производятся в объеме
                                                                -- ПП*Долю ИГС. Поэтому здесь мы вычисляем НП без учета доли ИГС.
                           vLoadIsn,
                           max(a.agrcurrisn) as agrcurrisn,
                           max(a.agrsharepc) as agrsharepc,
                           a.condisn,
                           a.statcode,
                           max(a.condpremagr) as premiumsum
                           ,vDateRep, repclassisn
                    FROM (
                          SELECT --+ordered use_nl ( ra a ) index ( a X_REPBUH2COND_AGRISN )
                                 a.bodyisn, a.agrisn,
                                 a.dateval,
                                 gcc2.gcc2(a.amount,a.buhcurrisn,a.agrcurrisn,a.dateval) as paidsum,
                                 a.agrcurrisn,
                                 nvl(ra.sharepc,100) as agrsharepc, a.condisn, a.statcode, a.condpremagr,
                                 x.repclassisn -- EGAO 29.05.2012
                          FROM repagr ra, repbuh2cond a, storages.v_rnprepclass x
                          WHERE ra.agrisn > vMinIsn and  ra.Agrisn <= vMaxIsn
                            AND months_between(ra.dateend+1,ra.datebeg)>13
                            AND a.Agrisn=ra.agrisn
                            AND a.statcode in (34,38, 221, 241)
                            AND a.sagroup in (1, 3, 2) -- EGAO 29.05.2012 AND a.sagroup in (1, 3)
                            and a.subaccisn not in (1022579925, 1017809225, 1022585025) -- EGAO 29.05.2012
                            and x.sagroup=a.sagroup
                         ) a
                    GROUP BY a.agrisn, a.dateval, a.statcode, a.condisn, a.repclassisn
                   ) a;
              COMMIT;
            END;';
      Parallel_Tasks.processtask(sesid,vsql);
      vCnt := vCnt+1;
      SYS.DBMS_APPLICATION_INFO.Set_Module ('rnp. fill rep_longagr_paysum',vCnt*LoadObjCnt);
      vMinIsn:=vMaxIsn;
    end loop;
    Parallel_Tasks.endsession(sesid);

  END;


  PROCEDURE make_rnp_msfo_all(pLoadIsn IN NUMBER := NULL)
  IS
    vLoadIsn NUMBER := nvl(pLoadIsn, GetActiveLoad(trunc(SYSDATE,'mm')-1));
    vDateRep DATE := GetDateRep(vLoadIsn);
    vMinIsn Number:=0;
    vMaxIsn Number := 0;
    vCnt Number:=0;
    sesid NUMBER;
    vSql Varchar2(4000);
    LoadObjCnt  constant Number := 10000;
    AgrLoadObjCnt  CONSTANT NUMBER := 100000;
    vPart VARCHAR(30);
  BEGIN
    IF vLoadIsn IS NULL  THEN
      raise_application_error(en_invalid_loadisn,'Invalid loadisn');
    END IF;
    IF vDateRep IS NULL  THEN
      raise_application_error(en_invalid_daterep,'Invalid daterep');
    END IF;

    vPart:=init_partition_by_key(pTableName => 'storages.rnp_msfo',pKey => vLoadIsn,pCompress => 1);

    replog_i (vLoadIsn, 'LoadRNPNEW', 'make_rnp_msfo_by_isn',pAction=>'Begin');
    SesId:=Parallel_Tasks.createnewsession();
    loop
      SELECT max (agrisn)
      into  vMaxIsn
      FROM (
            /*Select --+ index (b x_repbuh2cond_bodyisn)
                   b.bodyisn
            from repbuh2cond b
            WHERE b.bodyisn > vMinIsn
              and rownum <= LoadObjCnt*/
            Select --+ index (b X_REPBUH2COND_AGRISN)
                   b.agrisn
            from repbuh2cond b
            WHERE b.agrisn > vMinIsn
              and rownum <= LoadObjCnt  
           );

      if (vMaxIsn is null) then Exit; end if;

      vSql:=' Declare
                vLoadIsn    number :='||vLoadIsn||';
                vMinIsn     number := '||vMinIsn||';
                vMaxIsn     number := '||vMaxIsn||';
                vCnt        number :='||vCnt||';
              Begin
                dbms_application_info.Set_Module(''rnp_msfo'',''Thread: ''||vCnt);
                storages.report_rnp_new.make_rnp_msfo_by_isn(vLoadIsn, vMinIsn,vMaxIsn);
              end;';
      Parallel_Tasks.processtask(sesid,vsql);
      vMinIsn:=vMaxIsn;
      vCnt:=vCnt+1;
      dbms_application_info.Set_Module('rnp. fill rnp_msfo','Applied:'||vCnt*LoadObjCnt);
    end loop;
    Parallel_Tasks.endsession(sesid);
    replog_i (vLoadIsn, 'LoadRNPNEW', 'make_rnp_msfo_by_isn',pAction=>'End');

    vPart:=init_partition_by_key(pTableName => 'storages.rnp_msfo_r',pKey => vLoadIsn,pCompress => 1);

    replog_i (vLoadIsn, 'LoadRNPNEW', 'make_rnp_msfo_r_by_isn',pAction=>'Begin');
    SesId:=Parallel_Tasks.createnewsession();
    vMinIsn := -1;
    vCnt := 0;
    LOOP
      SELECT max (agrisn)
      INTO  vMaxIsn
      FROM (
            SELECT --+ index (b X_RNP_MSFO_AGR)
                   agrIsn
            FROM storages.rnp_msfo b
            WHERE b.agrisn > vMinIsn AND b.loadisn=vLoadIsn
              AND ROWNUM <= AgrLoadObjCnt
           );

      IF (vMaxIsn IS NULL) THEN EXIT; END IF;

      vSql:=' Declare
                vLoadIsn    number :='||vLoadIsn||';
                vMinIsn     number := '||vMinIsn||';
                vMaxIsn     number := '||vMaxIsn||';
                vCnt        number :='||vCnt||';
              Begin
                dbms_application_info.Set_Module(''rnp_msfo. fill rnp_msfo_r'',''Thread: ''||vCnt);
                storages.report_rnp_new.make_rnp_msfo_r_by_isn(vLoadIsn, vMinIsn, vMaxIsn);
              end;';
      Parallel_Tasks.processtask(sesid,vsql);
      vMinIsn:=vMaxIsn;
      vCnt:=vCnt+1;
      dbms_application_info.Set_Module('rnp_msfo. fill rnp_msfo_r','Applied:'||vCnt*AgrLoadObjCnt);
    END LOOP;

    Parallel_Tasks.endsession(sesid);
    replog_i (vLoadIsn, 'LoadRNPNEW', 'make_rnp_msfo_r_by_isn',pAction=>'End');
  END;

  PROCEDURE make_rnp_msfo_by_isn(pLoadIsn IN number, pMinIsn IN NUMBER, pMaxIsn IN NUMBER)
  IS
    vDateRep DATE := GetDateRep(pLoadIsn);
  BEGIN
    IF vDateRep IS NULL  THEN
      raise_application_error(en_invalid_daterep,'Invalid daterep');
    END IF;

    /*INSERT INTO rnp_msfo_buffer
        (loadisn, deptisn, agrisn, condisn, bodyisn, dateval, statcode,
         datebeg, dateend, rptgroupisn, formulaisn, ruleisnagr,
         rptclass,agrcurrisn,
         buhamountagr, agrdatebeg,agrdateend, \*isbazel,*\sagroup,
         riskclassisn, objisn, buhcurrisn, buhamount,
         condnachprem, condpremiumsum, agrpremiumsum, agrnachprem, isrevaluation, condformulaisn,
         agrclassisn, daterep,
         conddatebeg, --EGAO 06.12.2011
         conddateend --EGAO 06.12.2011
         , repclassisn -- EGAO 29.05.2012 
         )
    SELECT
           MAX(b.loadisn),
           b.deptisn, b.agrisn, b.condisn, b.bodyIsn, MAX(dateVal) AS dateval,
           MAX(b.statcode) AS statcode,
           MAX(b.datebeg) AS datebeg,
           MAX(b.dateend) AS dateend,
           b.rptgroupisn,
           MAX(b.agrformulaisn) AS agrformulaisn,
           MAX(b.ruleisnagr) AS ruleisnagr,
           b.rptclass,
           MAX(b.agrcurrisn) AS agrcurrisn,
           SUM(b.amountagr) AS buhamountagr,
           MAX(b.agrdatebeg) AS agrdatebeg,
           MAX(b.agrdateend) AS agrdateend,
           --b.IsBazEl,
           MAX(b.sagroup) AS sagroup,
           b.riskclassisn,
           b.objisn,
           MAX(b.buhcurrisn) AS buhcurrisn,
           SUM(b.amount) AS buhamount,
           MAX(b.condnachprem) AS condnachprem,
           MAX(b.condpremiumsum)AS  condpremiumsum,
           MAX(b.agrpremiumsum) AS agrpremiumsum,
           MAX(b.agrnachprem) AS agrnachprem,
           MAX(b.isrevaluation) AS isrevaluation,
           MAX(b.condformulaisn) AS condformulaisn,
           MAX(b.agrclassisn) AS agrclassisn,
           vDaterep,
           MAX(b.datebeg) AS datebeg, --EGAO 06.12.2011
           MAX(b.dateend) AS dateend --EGAO 06.12.2011
           , b.repclassisn -- EGAO 29.05.2012

    FROM (SELECT --+ no_merge ( b )
                 b.loadisn, b.deptisn, b.agrisn, b.condisn, b.bodyIsn, b.dateval,
                 CASE
                   WHEN b.agrclassisn IN (AgrInFacult,AgrInRetroc,AgrInOblig) AND Statcode=20102 THEN 32
                   ELSE Statcode
                 END AS  statcode,
                 b.datebeg, b.dateend, b.rptgroupisn, b.agrformulaisn, b.ruleisnagr,
                 b.rptclass, b.agrcurrisn, b.agrdatebeg, b.agrdateend, --b.IsBazEl,
                 b.sagroup, b.riskclassisn, b.objisn, b.buhcurrisn,
                 --{ EGAO 04.04.2012
                 \*CASE WHEN b.buhcurrisn=b.agrcurrisn THEN b.amount
                   ELSE gcc2.gcc2(b.amount,b.buhcurrisn,b.agrcurrisn,b.dateval)
                 END*\ 
                 CASE 
                   WHEN b.agrcurrisn IS NULL THEN CASE WHEN b.buhcurrisn=LocalCurr THEN b.amount ELSE gcc2.gcc2(b.amount,b.buhcurrisn,LocalCurr,vDaterep) END
                   WHEN b.buhcurrisn=b.agrcurrisn THEN b.amount
                   ELSE gcc2.gcc2(b.amount,b.buhcurrisn,b.agrcurrisn,b.dateval)
                 END AS amountagr,
                 --}EGAO 04.04.2012
                 b.amount,
                 b.isrevaluation, b.condformulaisn,
                 b.agrclassisn,
                 CASE WHEN b.condformulaisn=4 THEN (Select --+ Index (x X_REP_LONGAGR_PAYSUM_COND)
                                                           NVL(SUM(x.paidsum),0) --EGAO 12.09.2011 NVL(SUM(getcrosscover(x.paidsum,x.agrcurrisn,DollarCurr, b.conddaterate)),0)
                                                    from rep_longagr_paysum x
                                                    Where x.condisn=b.condisn
                                                      AND x.loadisn=b.loadisn
                                                      and x.dateval<=vDateRep
                                                      AND x.repclassisn=b.repclassisn -- EGAO 29.05.2012
                                                   )
                 END AS condnachprem,
                 CASE WHEN b.condformulaisn=4 THEN b.condpremagr\*EGAO 12.09.2011 condpremusd *\END AS condpremiumsum,
                 CASE WHEN b.agrformulaisn=4 THEN (SELECT --+ index ( x X_LONGAGRADDENDUM_AGRADD)
                                                        NVL(SUM(x.premiumsum),0)
                                                   FROM rep_longagraddendum x
                                                   WHERE x.agrisn=b.agrisn
                                                     AND least(nvl(x.datebeg,x.datesign),nvl(x.datesign, x.datebeg))<=vDateRep
                                                  )
                 END AS agrpremiumsum,
                 CASE WHEN b.agrformulaisn=4 THEN (SELECT --+ Index (x X_REP_LONGAGR_PAYSUM_AGR)
                                                          NVL(SUM(x.paidsum),0)
                                                   FROM rep_longagr_paysum x
                                                   WHERE x.agrisn=b.agrisn
                                                     AND x.loadisn=b.loadisn
                                                     AND x.dateval<=vDateRep
                                                     AND x.repclassisn=b.repclassisn -- EGAO 29.05.2012
                                                  )
                 END AS agrnachprem,
                 b.repclassisn
          FROM (SELECT b.*,
                       decode(b.deptisn, cargodept, decode(b.ruleisnagr, AgrCrgExhibition, 1, 2),
                                         decode(b.agrclassisn, AgrInOblig, 3,
                                                               decode (sign (months_between (b.agrdateend,b.agrdatebeg)-13),1,4,1))
                       ) AS agrformulaisn,
                       decode(b.deptisn, cargodept, decode(b.ruleisnagr, AgrCrgExhibition, 1, 2),
                                         decode(b.agrclassisn, AgrInOblig, 3,
                                                               decode (sign (months_between (b.dateend, b.datebeg)-13),1,4,1))
                             ) AS condformulaisn
                FROM (
                      SELECT --+ index (a x_repbuh2cond_Bodyisn)
                             a.bodyisn, a.deptisn, a.agrisn, a.condisn,
                             CASE
                               WHEN a.condisn=0 THEN trunc(a.datebeg)
                               WHEN trunc(a.datebegcond)>trunc(a.dateend) THEN trunc(a.dateend)
                               WHEN trunc(a.dateendcond)<trunc(a.datebeg) THEN trunc(a.datebeg)
                               WHEN trunc(a.datebegcond)<trunc(a.datebeg) THEN trunc(a.datebeg)
                               ELSE trunc(a.datebegcond)
                             END AS datebeg,
                             CASE
                               WHEN a.condisn=0 THEN trunc(a.dateend)
                               WHEN trunc(a.datebegcond)>trunc(a.dateend) THEN trunc(a.dateend)
                               WHEN trunc(a.dateendcond)<trunc(a.datebeg) THEN trunc(a.datebeg)
                               WHEN trunc(a.dateendcond)>trunc(a.dateend) THEN trunc(a.dateend)
                               ELSE trunc(a.dateendcond)
                             END AS dateend,
                             a.dateval, a.statcode, a.agrclassisn, a.rptgroupisn, a.buhcurrisn,
                             a.ruleisnagr, a.rptclass, a.amount, a.amountrub,
                             a.agrcurrisn,
                             trunc(a.datebeg) AS agrdatebeg,
                             trunc(a.dateend) AS agrdateend,
                             a.sagroup, a.riskclassisn, a.objisn,
                             a.condpremagr\*a.condpremusd*\\*EGAO 30.08.2011 rc.premusd AS condpremusd\*a.condpremusd*\*\, a.isrevaluation,
                             --rs.isbazel,
                             --coalesce(rc.agrdatebeg, rc.addsign, trunc(SYSDATE)) AS conddaterate,
                             pLoadIsn AS loadisn
                             , x.repclassisn -- EGAO 29.05.2012
                      FROM repbuh2cond a--, repsubject rs--, repcond rc
                           , v_rnprepclass x -- EGAO 29.05.2012
                      WHERE a.bodyisn >pMinIsn AND a.bodyisn<=pMaxIsn
                        AND a.dateval <= vDateRep --Интересуют только проводки до отчетной даты
                        AND a.statcode IN (38,221,34,241,32,20102,99)
                        AND a.saGroup IN (1,3,2) -- EGAO 29.05.2012 a.saGroup IN (1,3)

                        AND (
                              (a.deptisn = CargoDept And a.ruleisnagr<>AgrCrgExhibition and  dateval > vDateRep-21 and dateval <= vDateRep) -- 3 недели запрос Радченко 31.01.06
                                or (nvl(a.agrclassisn,0)=AgrInOblig and dateval > add_months (vDateRep,-12) and dateval <= vDateRep)
                                or ( (nvl(a.DeptIsn,0) not in (CargoDept) or a.ruleisnagr=AgrCrgExhibition)
                                     AND nvl (a.agrclassisn,0) <> AgrInOblig
                                     and trunc(a.dateend)>vDateRep
                                   )
                            )
                       --AND a.clientisn=rs.isn(+)
                       --AND rc.condisn(+)=a.condisn
                       -- EGAO 17.10.2011 AND trunc(a.datebeg)<=vDateRep -- !!! условие из нового ТЗ (см. алгортим расчета РНП МСФО по договорам п.2)
                       --{EGAO 17.10.2011 изменения в ТЗ
                       AND NVL(a.ruleisnagr,0)<>747261800
                       --}
                       --{EGAO 29.05.2012
                       AND x.sagroup=a.sagroup
                       AND a.subaccisn NOT IN (1022579925, 1017809225, 1022585025)
                       --}
                     ) b
               ) b
          --{EGAO 17.10.2011
          WHERE b.agrformulaisn=2 OR b.condformulaisn=2 OR b.agrdatebeg<=vDateRep
          --}
         ) b
    GROUP BY
             deptisn, agrisn, condisn, bodyIsn,
             rptgroupisn,
             rptclass,
             --isbazel,
             b.riskclassisn,
             b.objisn, b.repclassisn;*/

    
    INSERT INTO rnp_msfo_buffer
        (loadisn, deptisn, agrisn, condisn, bodyisn, dateval, statcode,
         datebeg, dateend, rptgroupisn, formulaisn, ruleisnagr,
         rptclass,agrcurrisn,
         buhamountagr, agrdatebeg,agrdateend, sagroup,
         riskclassisn, objisn, buhcurrisn, buhamount,
         condnachprem, condpremiumsum, agrpremiumsum, agrnachprem, isrevaluation, condformulaisn,
         agrclassisn, daterep,
         conddatebeg, --EGAO 06.12.2011
         conddateend --EGAO 06.12.2011
         , repclassisn -- EGAO 29.05.2012 
         , agrcomission -- EGAO 14.09.2012
         -- {EGAO 24.06.2013
         ,ruleisn 
         ,subaccisn
         ,parentobjclassisn
         ,IsSub
         ,clientjuridical
         ,clientorgformisn
         ,objclassisn 
         ,rptclassisn
         -- }EGAO 24.06.2013
         )
    
    /*WITH ps AS (SELECT --+ index ( x X_REP_LONGAGR_PAYSUM_AGR )
                  x.agrisn, 
                  x.condisn, 
                  x.repclassisn, 
                  SUM(x.paidsum) AS condpaidsum,
                  SUM(SUM(x.paidsum)) over (PARTITION BY x.agrisn, x.repclassisn) AS agrpaidsum
               FROM rep_longagr_paysum x
               WHERE x.agrisn > pMinIsn and x.agrisn<=pMaxIsn
                 AND x.dateval<=vDateRep 
                 AND x.statcode IN (34,38, 221, 241)
                 AND x.loadisn=pLoadIsn
                GROUP BY x.agrisn, x.condisn, x.repclassisn
              )*/
    SELECT --+ use_hash ( b x1 ) use_hash ( b x2 ) use_hash ( b x3 )
           pLoadIsn, nvl(b.deptisn,0), b.agrisn, b.condisn, b.bodyisn, b.dateval, b.statcode,
           b.datebeg, b.dateend, nvl(b.rptgroupisn,0), b.agrformulaisn, nvl(b.ruleisnagr,0),
           nvl(b.rptclass,0),b.agrcurrisn,
           b.buhamountagr, b.agrdatebeg,b.agrdateend, b.sagroup,
           b.riskclassisn, b.objisn, b.buhcurrisn, b.buhamount,
           CASE WHEN b.condformulaisn=4 THEN nvl(x2.condpaidsum,0) END AS condnachprem, 
           CASE WHEN b.condformulaisn=4 THEN condpremagr END AS condpremiumsum, 
           CASE WHEN b.agrformulaisn=4 THEN nvl(x3.premiumsum,0) END AS agrpremiumsum, 
           CASE WHEN b.agrformulaisn=4 THEN nvl(x1.agrpaidsum,0) END AS agrnachprem, 
           b.isrevaluation, b.condformulaisn,
           b.agrclassisn,
           vDaterep,
           b.conddatebeg, --EGAO 06.12.2011
           b.conddateend --EGAO 06.12.2011
           , b.repclassisn -- EGAO 29.05.2012 
           , b.agrcomission -- EGAO 
           -- {EGAO 24.06.2013
           ,nvl(b.ruleisn,0)
           ,nvl(b.subaccisn,0)
           ,nvl(b.parentobjclassisn,0) 
           ,b.IsSub
           ,nvl(b.clientjuridical,Case When nvl(b.DeptIsn,0)=707480016 Then 'N' else 'Y' end) -- взято из report2c.report_2c.PreLoad_Buh_Buffer_By_Isns
           ,nvl(b.clientorgformisn,0)
           ,nvl(b.objclassisn,0)
           ,nvl(b.rptclassisn,0)
           -- }EGAO 24.06.2013
    FROM (SELECT MAX(b.loadisn),
                 b.deptisn, b.agrisn, b.condisn, b.bodyIsn, MAX(dateVal) AS dateval,
                 MAX(b.statcode) AS statcode,
                 MAX(b.datebeg) AS datebeg,
                 MAX(b.dateend) AS dateend,
                 b.rptgroupisn,
                 MAX(b.agrformulaisn) AS agrformulaisn,
                 MAX(b.ruleisnagr) AS ruleisnagr,
                 b.rptclass,
                 MAX(b.agrcurrisn) AS agrcurrisn,
                 SUM(b.amountagr) AS buhamountagr,
                 MAX(b.agrdatebeg) AS agrdatebeg,
                 MAX(b.agrdateend) AS agrdateend,
                 MAX(b.sagroup) AS sagroup,
                 b.riskclassisn,
                 b.objisn,
                 MAX(b.buhcurrisn) AS buhcurrisn,
                 SUM(b.amount) AS buhamount,
                 MAX(b.condpremagr) AS condpremagr,
                 MAX(b.isrevaluation) AS isrevaluation,
                 MAX(b.condformulaisn) AS condformulaisn,
                 MAX(b.agrclassisn) AS agrclassisn,
                 vDaterep,
                 MAX(b.datebeg) AS conddatebeg, --EGAO 06.12.2011
                 MAX(b.dateend) AS conddateend --EGAO 06.12.2011
                 , b.repclassisn -- EGAO 29.05.2012
                 ,MAX(b.agrcomission) AS agrcomission -- EGAO 14.09.2012
                 -- {EGAO 24.06.2013
                 ,b.ruleisn 
                 ,b.subaccisn
                 ,b.parentobjclassisn 
                 ,MAX(b.IsSub) AS IsSub
                 ,MAX(b.clientjuridical) AS clientjuridical
                 ,MAX(b.clientorgformisn) AS clientorgformisn
                 ,b.objclassisn
                 ,b.rptclassisn
                 -- }EGAO 24.06.2013
          FROM (SELECT b.*,
                       --{EGAO 30.08.2012 в рамках ДИТ-12-3-172875
                       /*decode(b.deptisn, cargodept, decode(b.ruleisnagr, AgrCrgExhibition, 1, 2),
                                         decode(b.agrclassisn, AgrInOblig, 3,
                                                               decode (sign (months_between (b.agrdateend,b.agrdatebeg)-13),1,4,1))
                       )*/ 
                       CASE 
                         WHEN b.deptisn=cargodept THEN CASE WHEN b.ruleisnagr=AgrCrgExhibition THEN 1 ELSE 2 END
                         WHEN b.deptisn=CarrierDept AND b.ruleisnagr=TirRuleAgr THEN 5
                         WHEN b.agrclassisn=AgrInOblig THEN 3
                         WHEN sign (months_between (b.agrdateend,b.agrdatebeg)-13)=1 THEN 4
                         ELSE 1
                       END AS agrformulaisn,
                       /*decode(b.deptisn, cargodept, decode(b.ruleisnagr, AgrCrgExhibition, 1, 2),
                                         decode(b.agrclassisn, AgrInOblig, 3,
                                                               decode (sign (months_between (b.dateend, b.datebeg)-13),1,4,1))
                             ) */
                       CASE 
                         WHEN b.deptisn=cargodept THEN CASE WHEN b.ruleisnagr=AgrCrgExhibition THEN 1 ELSE 2 END
                         WHEN b.deptisn=CarrierDept AND b.ruleisnagr=TirRuleAgr THEN 5
                         WHEN b.agrclassisn=AgrInOblig THEN 3
                         WHEN sign (months_between (b.dateend,b.datebeg)-13)=1 THEN 4
                         ELSE 1
                       END  AS condformulaisn,
                       --} конец EGAO 30.08.2012 в рамках ДИТ-12-3-172875
                       CASE 
                         WHEN b.agrcurrisn IS NULL THEN CASE WHEN b.buhcurrisn=LocalCurr THEN b.amount ELSE gcc2.gcc2(b.amount,b.buhcurrisn,LocalCurr,vDaterep) END
                         WHEN b.buhcurrisn=b.agrcurrisn THEN b.amount
                         ELSE gcc2.gcc2(b.amount,b.buhcurrisn,b.agrcurrisn,b.dateval)
                       END AS amountagr
                FROM (
                      SELECT --+ index (a X_REPBUH2COND_AGRISN) use_hash ( ext ) no_merge ( ext ) use_hash ( ag ) no_merge ( ag )
                             a.bodyisn, a.deptisn, a.agrisn, a.condisn,
                             CASE
                               WHEN a.condisn=0 THEN trunc(a.datebeg)
                               WHEN trunc(a.datebegcond)>trunc(a.dateend) THEN trunc(a.dateend)
                               WHEN trunc(a.dateendcond)<trunc(a.datebeg) THEN trunc(a.datebeg)
                               WHEN trunc(a.datebegcond)<trunc(a.datebeg) THEN trunc(a.datebeg)
                               ELSE trunc(a.datebegcond)
                             END AS datebeg,
                             CASE
                               WHEN a.condisn=0 THEN trunc(a.dateend)
                               WHEN trunc(a.datebegcond)>trunc(a.dateend) THEN trunc(a.dateend)
                               WHEN trunc(a.dateendcond)<trunc(a.datebeg) THEN trunc(a.datebeg)
                               WHEN trunc(a.dateendcond)>trunc(a.dateend) THEN trunc(a.dateend)
                               ELSE trunc(a.dateendcond)
                             END AS dateend,
                             a.dateval, 
                             CASE
                               WHEN a.agrclassisn IN (AgrInFacult,AgrInRetroc,AgrInOblig) AND Statcode=20102 THEN 32
                               ELSE Statcode
                             END AS  statcode, 
                             a.agrclassisn, a.rptgroupisn, a.buhcurrisn,
                             a.ruleisnagr, a.rptclass, a.amount, a.amountrub,
                             a.agrcurrisn,
                             trunc(a.datebeg) AS agrdatebeg,
                             trunc(a.dateend) AS agrdateend,
                             a.sagroup, a.riskclassisn, a.objisn,
                             a.condpremagr, a.isrevaluation,
                             pLoadIsn AS loadisn
                             , x.repclassisn -- EGAO 29.05.2012
                             , a.comission/100 AS agrcomission -- EGAO 14.09.2012
                             -- {EGAO 24.06.2013
                             ,a.ruleisn 
                             ,a.subaccisn
                             ,a.parentobjclassisn 
                             ,decode(ext.agrisn,null,0,1) AS IsSub
                             ,ag.clientjuridical
                             ,ag.clientorgformisn
                             ,a.objclassisn
                             ,a.rptclassisn
                             -- }EGAO 24.06.2013
                      FROM repbuh2cond a, v_rnprepclass x, -- EGAO 29.05.2012
                           -- {EGAO 24.06.2013
                           (
                            SELECT --+ ordered use_nl ( ag sb ) index ( sb X_REPSUBJECT ) materialize
                                   ag.agrisn, ag.clientjuridical, sb.orgformisn AS clientorgformisn
                            FROM repagr ag, repSubject sb
                            WHERE ag.agrisn > pMinIsn and ag.agrisn <= pMaxIsn
                              and ag.clientisn=sb.isn(+)
                           ) ag,
                           (
                            SELECT --+ index ( ext X_AGREXT_AGR ) materialize
                                   DISTINCT ext.agrisn
                            FROM ais.agrext ext
                            WHERE ext.agrisn > pMinIsn and ext.agrisn <= pMaxIsn
                              AND ext.classisn=1071774425
                              AND ext.x1=1283168203
                           ) ext 
                           -- {EGAO 24.06.2013
                      WHERE a.agrisn >pMinIsn AND a.agrisn<=pMaxIsn
                        AND a.dateval <= vDateRep --Интересуют только проводки до отчетной даты
                        AND a.statcode IN (38,221,34,241,32,20102,99)
                        AND a.saGroup IN (1,3,2) -- EGAO 29.05.2012 a.saGroup IN (1,3)

                        AND (
                              (a.deptisn = CargoDept And a.ruleisnagr<>AgrCrgExhibition and  dateval > vDateRep-21 and dateval <= vDateRep) -- 3 недели запрос Радченко 31.01.06
                                or (nvl(a.agrclassisn,0)=AgrInOblig and dateval > add_months (vDateRep,-12) and dateval <= vDateRep)
                                or (a.DeptIsn=742950000 AND a.ruleisnagr=TirRuleAgr AND dateval > add_months (vDateRep,-3) and dateval <= vDateRep) -- EGAO 03.04.2013
                                or ( (nvl(a.DeptIsn,0) not in (CargoDept) or a.ruleisnagr=AgrCrgExhibition)
                                     AND nvl (a.agrclassisn,0) <> AgrInOblig
                                     AND (nvl(a.DeptIsn,0)<>742950000 OR nvl(a.ruleisnagr,0)<>TirRuleAgr) -- EGAO 03.04.2013
                                     and trunc(a.dateend)>vDateRep
                                   )
                            )
                       -- EGAO 17.10.2011 AND trunc(a.datebeg)<=vDateRep -- !!! условие из нового ТЗ (см. алгортим расчета РНП МСФО по договорам п.2)
                       --{EGAO 17.10.2011 изменения в ТЗ
                       AND NVL(a.ruleisnagr,0)<>747261800
                       --}
                       --{EGAO 29.05.2012
                       AND x.sagroup=a.sagroup
                       AND a.subaccisn NOT IN (1022579925, 1017809225, 1022585025)
                       --}
                       --{ EGAO 24.06.2013
                       AND ext.agrisn(+)=a.agrisn
                       AND ag.agrisn(+)=a.agrisn
                       --} EGAO 24.06.2013
                     ) b
               ) b
          --{EGAO 17.10.2011
          WHERE b.agrformulaisn=2 OR b.condformulaisn=2 OR b.agrdatebeg<=vDateRep
          GROUP BY  deptisn, agrisn, condisn, bodyIsn, rptgroupisn, rptclass, b.riskclassisn, b.objisn, b.repclassisn
                    -- { EGAO 24.06.2013
                    ,b.ruleisn 
                    ,b.subaccisn
                    ,b.parentobjclassisn
                    ,b.objclassisn
                    ,b.rptclassisn
                    -- } EGAO 24.06.2013 
         ) b,
         /*(SELECT DISTINCT agrisn, repclassisn, agrpaidsum FROM ps) x1,
         ps x2,*/
         
         (SELECT --+ index ( x X_REP_LONGAGR_PAYSUM_AGR )
                  x.agrisn, 
                  x.condisn, 
                  x.repclassisn, 
                  SUM(x.paidsum) AS condpaidsum
               FROM rep_longagr_paysum x
               WHERE x.agrisn > pMinIsn and x.agrisn<=pMaxIsn
                 AND x.dateval<=vDateRep 
                 AND x.statcode IN (34,38, 221, 241)
                 AND x.loadisn=pLoadIsn
                GROUP BY x.agrisn, x.condisn, x.repclassisn
              ) x2,
              (SELECT --+ index ( x X_REP_LONGAGR_PAYSUM_AGR )
                  x.agrisn, 
                  x.repclassisn, 
                  SUM(x.paidsum) AS agrpaidsum
               FROM rep_longagr_paysum x
               WHERE x.agrisn > pMinIsn and x.agrisn<=pMaxIsn
                 AND x.dateval<=vDateRep 
                 AND x.statcode IN (34,38, 221, 241)
                 AND x.loadisn=pLoadIsn
                GROUP BY x.agrisn, x.repclassisn
              ) x1,
         
         
         (SELECT --+ index ( x X_LONGAGRADDENDUM_AGR)
                 agrisn, SUM(x.premiumsum) AS premiumsum
          FROM rep_longagraddendum x
          WHERE x.agrisn > pMinIsn and x.agrisn<=pMaxIsn
            AND least(nvl(x.datebeg,x.datesign),nvl(x.datesign, x.datebeg))<=vDateRep
          GROUP BY x.agrisn  
         ) x3
    WHERE x2.agrisn(+)=b.agrisn
      AND x2.condisn(+)=b.condisn
      AND x2.repclassisn(+)=b.repclassisn
      AND x1.agrisn(+)=b.agrisn
      AND x1.repclassisn(+)=b.repclassisn
      AND x3.agrisn(+)=b.agrisn;
    
     
      update rnp_msfo_buffer
      set rnpshare = decode (formulaisn,2, 0.5,
                                        3, greatest (0,(3.5-trunc (months_between (vDateRep, trunc (DateVal,'MM'))/3))/4 ),--1/8 na god
                                        5, greatest (0,(0.5-trunc (months_between (vDateRep, trunc (DateVal,'MM'))/3))) -- EGAO 30.08.2012 в рамках ДИТ-12-3-172875
                                        ),
        Agrrnpshare =decode (formulaisn,2, 0.5,
                                        3, greatest (0,(3.5-trunc (months_between (vDateRep, trunc (DateVal,'MM'))/3))/4 ),--1/8 na god
                                        5, greatest (0,(0.5-trunc (months_between (vDateRep, trunc (DateVal,'MM'))/3))) -- EGAO 30.08.2012 в рамках ДИТ-12-3-172875
                            )
      where formulaisn IN (2, 3, 5)
      and dateval between add_months (vDateRep,-decode (formulaisn,2,1,3,12,5,3))+1 and vDateRep;


      --Подгоняем дату окончания для длинных договоров
      update rnp_msfo_buffer A
      set
       DateEnd = CASE NVL(a.condpremiumsum,0)
                   WHEN 0 THEN TRUNC(a.DateEnd) -- 1
                   ELSE
                     CASE sign(round(nvl(a.condnachprem, 0),10)/a.condpremiumsum)--EGAO 16.12.2011 sign(nvl(a.condnachprem, 0)/a.condpremiumsum)
                       WHEN -1 THEN TRUNC(a.DateEnd) -- 2
                       ELSE
                         CASE sign((nvl(a.condnachprem,0)/a.condpremiumsum)-1)
                           WHEN 1 THEN TRUNC(a.DateEnd) -- 3
                           ELSE TRUNC(TRUNC(a.DATEBEG)-1+Greatest(1,(TRUNC(a.DATEEND)+1-TRUNC(a.DATEBEG))*(NVL(a.condnachprem,0)/a.condpremiumsum))
                                     ) -- 4 и 5
                         END
                     END
                 END,
       AgrDateEnd =  CASE NVL(a.agrpremiumsum, 0)
                       WHEN 0 THEN TRUNC(a.agrdateend) -- 1
                       ELSE
                         CASE sign(round(nvl(a.agrnachprem,0),10)/a.agrpremiumsum)--EGAO 16.12.2011 sign(nvl(a.agrnachprem,0)/a.agrpremiumsum)
                           WHEN -1 THEN TRUNC(a.agrdateend) -- 2
                           ELSE
                             CASE sign((nvl(a.agrnachprem,0)/a.agrpremiumsum)-1)
                               WHEN 1 THEN TRUNC(a.agrdateend)  -- 3
                               ELSE TRUNC(TRUNC(a.AgrDATEBEG)-1+Greatest(1,(TRUNC(a.AgrDATEEND)+1-TRUNC(a.AgrDATEBEG))*(nvl(a.agrnachprem,0)/a.agrpremiumsum ))
                                         ) -- 4 и 5
                             END
                         END
                     END
      where formulaisn = 4;

      --Классическая Pro Rata
      --{EGAO 16.12.2011
      /*update rnp_msfo_buffer
      set
       rnpshare = (dateend-vDateRep)/(dateend-datebeg+1)
      where formulaisn in (1,4)
        And   (dateend >vDateRep And datebeg<=vDateRep);


      update rnp_msfo_buffer
      set
          AGRRNPSHARE = (Agrdateend-vDateRep)/(Agrdateend-Agrdatebeg+1)
      where formulaisn in (1,4)
      And  (AgrDateEnd>vDaterep And AgrDateBeg<=vDateRep);*/


      update rnp_msfo_buffer
      SET AGRRNPSHARE = greatest(0,(Agrdateend-vDateRep)/(Agrdateend-Agrdatebeg+1)),
          rnpshare = greatest(0,decode (sign (datebeg-vDateRep),1,1, (dateend-vDateRep)/(dateend-datebeg+1)))
      where formulaisn in (1,4)
        And AgrDateBeg<=vDateRep;
      --}


      Insert Into rnp_msfo(
        loadisn, daterep, deptisn, agrisn, bodyisn, statcode, rptgroupisn, ruleisnagr, rptclass, agrcurrisn,
        datebeg, dateend, buhamountagr,  rnpshare, AgrRnpShare, IsBazEl, sagroup, riskclassisn, objisn, condisn,
           buhcurrisn, dateval, buhamount, isrevaluation, agrclassisn, agrdatebeg, agrdateend,
           formulaisn, condnachprem, condpremiumsum, agrnachprem, agrpremiumsum
           ,repclassisn -- EGAO 29.05.2012
           ,agrcomission -- EGAO 14.09.2012
           -- {EGAO 24.06.2013
           ,ruleisn 
           ,subaccisn
           ,parentobjclassisn
           ,IsSub
           ,clientjuridical 
           ,clientorgformisn 
           ,objclassisn
           -- }EGAO 24.06.2013
           )
      Select a.loadisn,
             a.daterep,
             a.deptisn,
             a.agrisn,
             a.BodyIsn,
             a.statcode,
             a.rptgroupisn,
             a.ruleisnagr,
             a.rptclass,
             a.agrcurrisn,
             a.conddatebeg,--EGAO 06.12.2011 a.datebeg,
             a.conddateend,--EGAO 06.12.2011 a.Dateend,
             a.buhamountagr,
             a.rnpshare,-- EGAO 02.08.2011 rnpshare*buhamount/buhamount,
             a.Agrrnpshare,-- EGAO 02.08.2011 Agrrnpshare*buhamount/buhamount,
             a.IsBazEl,a.sagroup,
             a.riskclassisn, a.objisn, a.condisn,
             a.buhcurrisn, dateval, a.buhamount,
             a.isrevaluation,
             a.agrclassisn, a.agrdatebeg, a.agrdateend, a.formulaisn, a.condnachprem, a.condpremiumsum
             , a.agrnachprem, a.agrpremiumsum -- EGAO 16.12.2011
             , a.repclassisn -- EGAO 29.05.2012
             ,a.agrcomission -- EGAO 14.09.2012
             -- {EGAO 24.06.2013
             ,a.ruleisn 
             ,a.subaccisn
             ,a.parentobjclassisn
             ,a.IsSub
             ,a.clientjuridical 
             ,a.clientorgformisn 
             ,a.objclassisn
             -- }EGAO 24.06.2013
      from rnp_msfo_buffer a
      Where (RnpShare>0 or AgrRnpShare>0)
      AND buhamountagr<>0 ;

      COMMIT;


  END;


  PROCEDURE make_rnp_msfo_r_by_isn(pLoadIsn IN NUMBER, pMinIsn IN NUMBER, pMaxIsn IN NUMBER)
  IS
    vDateRep DATE := GetDateRep(pLoadIsn);

    CURSOR  a(curMinIsn NUMBER, curMaxIsn NUMBER, curLoadIsn NUMBER)
    IS
    select x.*, count(1) over () as cnt, rownum rn
    from (
    SELECT --+ index ( x X_RNP_MSFO_AGR )
           x.agrisn, max(x.loadisn) as loadisn
    FROM rnp_msfo x
    WHERE x.agrisn> curMinIsn AND x.agrisn <= curMaxIsn
      and x.loadisn=curLoadIsn
    GROUP BY x.agrisn) x;

    --EGAO 05.12.2011
    TYPE TTab IS TABLE OF a%ROWTYPE INDEX BY BINARY_INTEGER;
    Tab TTab;

  BEGIN
    IF vDateRep IS NULL  THEN
      raise_application_error(en_invalid_daterep,'Invalid daterep');
    END IF;

    OPEN a(pMinIsn, pMaxIsn, pLoadIsn);
    LOOP
       FETCH a BULK COLLECT INTO Tab LIMIT 100;
       EXIT WHEN Tab.count=0;
       FOR i in Tab.FIRST..Tab.LAST LOOP

         dbms_application_info.set_module('rnp_msfo_r',Tab(i).rn ||' of '||Tab(i).cnt);

         INSERT INTO rnp_msfo_r(
           loadisn, daterep, agrisn,condisn, deptisn, isrevaluation, rptclass, rptgroupisn, k_rnp_msfoagr,
           k_rnp_msfocond, rnp_msfocond, rnp_msfoagr, sagroup, dacagr, daccond, conddatebeg, conddateend,
           agrid, agrdatebeg, agrdateend, agrclassisn, agrcurrisn, agrcomission, agrruleisn, premamount
           , repclassisn)
        SELECT loadisn,
               daterep,
               agrisn,
               condisn,
               deptisn,
               isrevaluation,
               rptclass,
               rptgroupisn,
               k_rnp_msfoagr,
               k_rnp_msfocond,
               rnp_msfocond,
               rnp_msfoagr,
               r.sagroup,
               CASE WHEN r.agrclassisn=9020 THEN r.comrein*r.dacagrrnpshare ELSE r.agrcomission*r.rnp_msfoagr/100 END AS dacagr,
               CASE WHEN r.agrclassisn=9020 THEN r.comrein*r.daccondrnpshare ELSE r.agrcomission*r.rnp_msfocond/100 END AS daccond,
               r.conddatebeg,
               r.conddateend,
               r.agrid,
               r.agrdatebeg,
               r.agrdateend,
               r.agrclassisn,
               r.agrcurrisn,
               r.agrcomission,
               r.agrruleisn,
               r.premamount
               , r.repclassisn -- EGAO 29.05.2012
        FROM (SELECT --+ index ( r  X_RNP_MSFO_AGR ) ordered use_nl ( r ra )
                     r.loadisn,
                     MAX(r.daterep) AS daterep, r.deptisn, r.agrisn, r.condisn, r.rptclass, r.rptgroupisn, r.isrevaluation, r.sagroup,
                     MAX(r.agrrnpshare) AS k_rnp_msfoagr,
                     MAX(r.rnpshare) AS k_rnp_msfocond,
                     SUM(CASE WHEN r.Statcode IN (38,34,221,241) THEN r.buhamountagr*r.agrrnpshare end) AS rnp_msfoagr,
                     SUM(CASE WHEN r.Statcode IN (38,34,221,241) THEN r.buhamountagr*r.rnpshare end) AS rnp_msfocond,
                     -SUM(Decode(r.Statcode,32,r.buhamountagr,0)) comrein,
                     SUM(CASE WHEN r.Statcode IN (32) THEN buhamountagr*AgrRNPSHARE end)/
                     (
                      case when Sum( Case When r.Statcode In (32) Then buhamountagr end) =0 then 1
                      else Sum( Case When r.Statcode In (32) Then buhamountagr end) end
                     ) DacAgrRNPSHARE,
                     SUM(CASE WHEN r.Statcode IN (32) THEN buhamountagr*rnpshare end)/
                     (
                      case when Sum( Case When r.Statcode In (32) Then buhamountagr end) =0 then 1
                      else Sum( Case When r.Statcode In (32) Then buhamountagr end) end
                     ) DacCondRNPSHARE,
                     MAX(r.formulaisn) AS formulaisn,
                     MAX(trunc(r.datebeg)) as conddatebeg,
                     MAX(trunc(r.dateend)) as conddateend,
                     MAX(ra.id) AS agrid,
                     MAX(trunc(ra.datebeg)) AS agrdatebeg,
                     MAX(trunc(ra.dateend)) AS agrdateend,
                     MAX(ra.classisn) AS agrclassisn,
                     MAX(ra.currisn) AS agrcurrisn,
                     MAX(ra.comission) AS agrcomission,
                     MAX(ra.ruleisn) AS agrruleisn,
                     SUM(CASE WHEN r.Statcode IN (38,34,221,241) THEN r.buhamountagr END) AS premamount
                     , r.repclassisn -- EGAO 29.05.2012
              FROM storages.rnp_msfo r, repagr ra
              WHERE r.loadisn=Tab(i).loadisn
                and r.agrisn=Tab(i).agrisn
                AND ra.agrisn(+)=r.agrisn
              GROUP BY r.loadisn, r.deptisn, r.agrisn, r.condisn, r.rptclass, r.rptgroupisn, r.isrevaluation, r.sagroup
                       , r.repclassisn -- EGAO 29.05.2012
              HAVING (SUM(CASE WHEN r.Statcode IN (38,34,221,241) THEN r.buhamountagr*r.agrrnpshare end)<>0 OR
                      SUM(CASE WHEN r.Statcode IN (38,34,221,241) THEN r.buhamountagr*r.rnpshare end)<>0
                     )
             ) r;
       END LOOP;
       COMMIT;
    END LOOP;
    COMMIT;
    IF a%ISOPEN THEN
      CLOSE a;
    END IF;
  END;



  PROCEDURE make_rnp_rsbu_all(pLoadIsn IN NUMBER := NULL)
  IS
    vLoadIsn NUMBER := nvl(pLoadIsn, GetActiveLoad(trunc(SYSDATE,'mm')-1));
    vDateRep DATE := GetDateRep(vLoadIsn);
    LoadObjCnt     NUMBER := 10000;
    AgrLoadObjCnt  NUMBER := 100000;
    vMinIsn     NUMBER :=0;
    vMaxIsn     NUMBER := 0;
    vCnt        NUMBER :=0;
    sesid       NUMBER;
    vSql        VARCHAR2(4000);
    vPart       VARCHAR2(30);
  BEGIN
    IF vLoadIsn IS NULL  THEN
      raise_application_error(en_invalid_loadisn,'Invalid loadisn');
    END IF;
    IF vDateRep IS NULL  THEN
      raise_application_error(en_invalid_daterep,'Invalid daterep');
    END IF;  
    
    vPart:=init_partition_by_key(pTableName => 'storages.rnp_rsbu',pKey => vLoadIsn,pCompress => 1);

    --!!!
    Execute Immediate 'ALTER TABLE storages.rnp_rsbu MODIFY PARTITION '||vPart||' UNUSABLE LOCAL INDEXES';
    --!!!                     

    replog_i (vLoadIsn, 'LoadRNPNEW', 'make_rnp_rsbu_by_isn',pAction=>'Begin');
    SesId:=Parallel_Tasks.createnewsession();
    LOOP
      SELECT max (isn)
      INTO vMaxIsn
      FROM (
            /*SELECT --+ index (b x_repbuh2cond)
                   Isn
            FROM repbuh2cond b
            WHERE isn > vMinIsn
              AND ROWNUM <= LoadObjCnt*/
            SELECT --+ index (b X_REPBUH2COND_AGRISN)
                   b.agrisn AS isn
            FROM repbuh2cond b
            WHERE b.agrisn > vMinIsn
              AND ROWNUM <= LoadObjCnt  
           );

      IF (vMaxIsn IS NULL) THEN EXIT; END IF;

      vSql:=' Declare
                vloadisn    number := '||vLoadIsn||';
                vMinIsn     number := '||vMinIsn||';
                vMaxIsn     number := '||vMaxIsn||';
                vCnt        number :='||vCnt||';
              Begin
                dbms_application_info.Set_Module(''rnp_rsbu'',''Thread: ''||vCnt);
                storages.report_rnp_new.make_rnp_rsbu_by_isn(vloadisn,vMinIsn,vMaxIsn);
              end;';
      Parallel_Tasks.processtask(sesid,vsql);
      vMinIsn:=vMaxIsn;
      vCnt:=vCnt+1;
      dbms_application_info.Set_Module('rnp. fill rnp_rsbu','Applied:'||vCnt*LoadObjCnt);
    END LOOP;
    Parallel_Tasks.endsession(sesid);
    REBULD_TABLE_INDEX('storages.rnp_rsbu', PPARTITITON=>vPart);
    replog_i (vLoadIsn, 'LoadRNPNEW', 'make_rnp_rsbu_by_isn',pAction=>'End');



    vPart:=init_partition_by_key(pTableName => 'storages.rnp_rsbu_r',pKey => vLoadIsn,pCompress => 1);

    replog_i (vLoadIsn, 'LoadRNPNEW', 'make_rnp_rsbu_r_by_isn',pAction=>'Begin');
    SesId:=Parallel_Tasks.createnewsession();
    vMinIsn := -1;
    vCnt := 0;
    LOOP
      SELECT max (agrisn)
      INTO  vMaxIsn
      FROM (
            SELECT --+ index (b X_RNP_RSBU_AGRISN)
                   agrIsn
            FROM storages.rnp_rsbu b
            WHERE agrisn > vMinIsn AND LoadIsn=vLoadIsn
              AND ROWNUM <= AgrLoadObjCnt
           );

      IF (vMaxIsn IS NULL) THEN EXIT; END IF;

      vSql:=' Declare
                vloadisn    number := '||vLoadIsn||';
                vMinIsn     number := '||vMinIsn||';
                vMaxIsn     number := '||vMaxIsn||';
                vCnt        number :='||vCnt||';
              Begin
                dbms_application_info.Set_Module(''rnp_rsbu. fill rnp_rsbu_r'',''Thread: ''||vCnt);
                storages.report_rnp_new.make_rnp_rsbu_r_by_isn(vLoadIsn, vMinIsn, vMaxIsn);
              end;';
      Parallel_Tasks.processtask(sesid,vsql);
      vMinIsn:=vMaxIsn;
      vCnt:=vCnt+1;
      dbms_application_info.Set_Module('rnp_rsbu. fill rnp_rsbu_r','Applied:'||vCnt*AgrLoadObjCnt);
    END LOOP;

    Parallel_Tasks.endsession(sesid);
    replog_i (vLoadIsn, 'LoadRNPNEW', 'make_rnp_rsbu_r_by_isn',pAction=>'End');

    --EGAO 14.06.2013
    make_resrnpsummary(vLoadIsn);
    
  END;



  PROCEDURE make_rnp_rsbu_by_isn(pLoadIsn IN NUMBER, pMinIsn IN NUMBER, pMaxIsn IN NUMBER)
  IS
    vDateRep DATE := GetDateRep(pLoadIsn);
    vMinIsn Number:=pMinIsn;
    vMaxIsn Number := pMaxIsn;
    vEuroRate NUMBER;
    vDollarRate NUMBER;
  Begin
    IF vDateRep IS NULL THEN
      raise_application_error(en_invalid_daterep,'Invalid daterep');
    END IF;

    vEuroRate := GetCrossCover (1,EuroCurr,LocalCurr,vDateRep);
    vDollarRate := GetCrossCover (1,DollarCurr,LocalCurr,vDateRep);

    insert into rnp_rsbu_buffer
      (buhcondisn, deptisn, statcode, agrisn, bodyisn, condisn,
      rptgroupisn, datebeg, dateend, comission,
      buhamount, buhamountrub, buhpc, isrevaluation,
      dateval, currisn, agrformulaisn,ruleisnagr,budgetgroupisn,sagroup, agrcurrisn, objisn, riskclassisn,
      rptclass, conddatebeg, conddateend, condformulaisn, objclassisn,  -- EGAO 26.01.2011
      agrnachprem, -- EGAO 26.01.2011
      agrpremiumsum, -- EGAO 26.01.2011
      condnachprem, condpremiumsum    -- EGAO 26.01.2011
      , repclassisn -- EGAO 29.05.2012
      -- { EGAO 24.06.2013
      ,ruleisn 
      ,subaccisn
      ,parentobjclassisn
      ,IsSub
      ,clientjuridical 
      ,clientorgformisn 
      ,rptclassisn 
      -- } EGAO 24.06.2013
      --{EGAO 19.07.2013
      ,agrclassisn
      ,istender
      --}EGAO 19.07.2013
      )
    SELECT b.isn, nvl(b.deptisn,0), b.statcode, b.agrisn, b.bodyisn, b.condisn, nvl(b.rptgroupisn,0),
            b.datebeg, b.dateend,
            b.comission,
            b.buhamount, b.buhamountrub, b.buhpc, b.isrevaluation, b.dateval,
            b.buhcurrisn,
            b.agrformulaisn,
            nvl(b.ruleisnagr,0),
            b.motivgroupisn, b.sagroup, b.agrcurrisn, b.objisn, b.riskclassisn,
            nvl(b.rptclass,0), b.datebegcond, b.dateendcond, b.condformulaisn, nvl(b.objclassisn,0),
            b.agrnachprem, b.agrpremiumsum,
            CASE WHEN b.condisn=0 THEN b.agrnachprem ELSE b.condnachprem END AS condnachprem,
            CASE WHEN b.condisn=0 THEN b.agrpremiumsum ELSE b.condpremiumsum END AS condpremiumsum
            , b.repclassisn -- EGAO 29.05.2012
            -- { EGAO 24.06.2013
            ,nvl(b.ruleisn,0)
            ,nvl(b.subaccisn,0)
            ,nvl(b.parentobjclassisn,0)
            ,b.IsSub
            ,nvl(b.clientjuridical,Case When nvl(b.DeptIsn,0)=707480016 Then 'N' else 'Y' end) -- взято из report2c.report_2c.PreLoad_Buh_Buffer_By_Isns
            ,nvl(b.clientorgformisn,0)
            ,nvl(b.rptclassisn,0)
            -- } EGAO 24.06.2013
            -- { EGAO 19.07.2013
            ,agrclassisn 
            ,istender
            -- }EGAO 19.07.2013
    FROM (SELECT  isn, deptisn, statcode, agrisn, bodyisn, condisn, rptgroupisn,
                   datebeg, dateend,
                   comission,
                   buhamount, buhamountrub, buhpc, isrevaluation, dateval,
                   buhcurrisn,
                   agrformulaisn,
                   ruleisnagr,
                   motivgroupisn,sagroup, agrcurrisn, b.objisn,b.riskclassisn,
                   b.rptclass,
                   b.datebegcond,
                   b.dateendcond,
                   condformulaisn,
                   b.objclassisn,
                   CASE b.agrformulaisn
                     WHEN 8 THEN /*(SELECT \*+ index ( x X_REP_LONGAGR_PAYSUM_AGR )*\
                                         NVL(SUM(x.paidsum),0)
                                  FROM rep_longagr_paysum x
                                  WHERE x.agrisn=b.agrisn AND x.dateval<=vDateRep AND x.statcode IN (34, 38)
                                    AND x.loadisn=pLoadIsn
                                    AND x.repclassisn=b.repclassisn -- EGAO 29.05.2012
                                 )*/b.agrpaidsum -- EGAO 30.05.2012
                   END AS agrnachprem,
                   CASE b.agrformulaisn
                     WHEN 8 THEN /*(SELECT NVL(SUM(x.premiumsum),0)
                                  FROM rep_longagraddendum x
                                  WHERE x.agrisn=b.agrisn
                                    AND \*nvl(x.datebeg, x.datesign)*\
                                    least(nvl(x.datebeg, x.datesign), nvl(x.datesign, x.datebeg)) <= vDateRep
                                 )*/b.agrpremiumsum
                   END AS agrpremiumsum,
                   CASE b.condformulaisn
                     WHEN 7 THEN /*(SELECT \*+ index ( x X_REP_LONGAGR_PAYSUM_COND )*\
                                         NVL(SUM(x.paidsum), 0)-- EGAO 13.09.2011 NVL(SUM(getcrosscover(x.paidsum,x.agrcurrisn,DollarCur, b.conddaterate)),0)
                                  FROM rep_longagr_paysum x
                                  WHERE x.condisn=b.condisn
                                    AND x.dateval<=vDateRep
                                    AND x.statcode IN (34, 38)
                                    AND x.loadisn=pLoadIsn
                                    AND x.repclassisn=b.repclassisn -- EGAO 29.05.2012
                                 )*/b.condpaidsum -- EGAO 30.05.2012
                   END AS condnachprem,
                   CASE b.condformulaisn
                     WHEN 7 THEN b.condpremagr -- EGAO 13.09.2011 b.condpremusd
                   END AS condpremiumsum
                   , b.repclassisn -- EGAO 29.05.2012
                   -- { EGAO 24.06.2013
                   ,b.ruleisn 
                   ,b.subaccisn
                   ,b.parentobjclassisn
                   ,b.IsSub
                   ,b.clientjuridical 
                   ,b.clientorgformisn
                   ,b.rptclassisn  
                   -- } EGAO 24.06.2013
                   ,b.agrclassisn -- EGAO 19.07.2013
                   ,b.istender -- EGAO 19.07.2013
           FROM  (
                  /*WITH ps AS (SELECT --+ index ( x X_REP_LONGAGR_PAYSUM_AGR )
                                x.agrisn, 
                                x.condisn, 
                                x.repclassisn, 
                                SUM(x.paidsum) AS condpaidsum,
                                SUM(SUM(x.paidsum)) over (PARTITION BY x.agrisn, x.repclassisn) AS agrpaidsum
                             FROM rep_longagr_paysum x
                             WHERE x.agrisn > vMinIsn and x.agrisn<=vMaxIsn
                               AND x.dateval<=vDateRep 
                               AND x.statcode IN (34, 38)
                               AND x.loadisn=pLoadIsn
                              GROUP BY x.agrisn, x.condisn, x.repclassisn
                            )*/
                  SELECT --+ use_hash ( b x1 ) use_hash ( b x2 ) use_hash ( b x3 ) use_hash ( b tdr )
                         b.isn, b.deptisn, b.statcode, b.agrisn, b.bodyisn, b.condisn, b.rptgroupisn,
                         b.datebeg, b.dateend,
                         b.comission,
                         b.buhamount, b.buhamountrub, b.buhpc, b.isrevaluation, b.dateval,
                         b.buhcurrisn,
                         decode (b.rptgroupisn,755075000,1,755078500,1,
                                             747777600,2,747777700,2,755042100,2,755059900,2,755041800,2,
                                             755046000,3,
                                             755041200,4,
                                             755040800,5,
                                             decode(sign (months_between(b.DateEnd, b.DateBeg)-13),1,decode (RULEISNAGR,37504416,7,49420116,7,52243216,7,821224101,7,8),6)
                                ) AS agrformulaisn,
                         b.ruleisnagr,
                         b.motivgroupisn, b.sagroup, b.agrcurrisn, b.objisn,b.riskclassisn,
                         b.rptclass,
                         b.datebegcond,
                         b.dateendcond,
                         decode (b.rptgroupisn,755075000,1,755078500,1,
                                             747777600,2,747777700,2,755042100,2,755059900,2,755041800,2,
                                             755046000,3,
                                             755041200,4,
                                             755040800,5,
                                 decode(sign (months_between(b.dateendcond,b.datebegcond)-13),1,7,6)
                                ) AS condformulaisn,
                         b.objclassisn,
                         b.condpremagr --EGAO 13.09.2011 b.condpremusd,
                         --EGAO 13.09.2011 ,b.conddaterate
                         , b.repclassisn -- EGAO 29.05.2012
                         , nvl(x2.condpaidsum,0) AS condpaidsum, nvl(x1.agrpaidsum,0) AS agrpaidsum, nvl(x3.premiumsum,0) AS agrpremiumsum -- EGAO 30.05.2012
                         -- { EGAO 24.06.2013
                         ,b.ruleisn 
                         ,b.subaccisn
                         ,b.parentobjclassisn
                         ,b.IsSub 
                         ,b.clientjuridical 
                         ,b.clientorgformisn
                         ,b.rptclassisn
                         -- } EGAO 24.06.2013
                         ,b.agrclassisn -- EGAO 19.07.2013
                         ,nvl(tdr.istender,0) AS istender -- EGAO 19.07.2013
                  from (
                        SELECT 1 AS isn,
                               b.deptisn, b.agrisn, b.condisn, b.bodyIsn, b.rptgroupisn, 
                               b.rptclass, b.riskclassisn, b.objisn, motivgroupisn, 
                               b.objclassisn
                               ,MAX(b.statcode) AS statcode
                               ,MAX(b.datebeg) AS datebeg
                               ,MAX(b.dateend) AS dateend
                               ,MAX(b.comission) AS comission
                               ,MAX(b.buhamount) AS buhamount
                               ,MAX(b.buhamountrub) AS buhamountrub
                               ,SUM(b.buhpc) AS buhpc
                               ,MAX(b.isrevaluation) AS isrevaluation
                               ,MAX(b.dateval) AS dateval
                               ,MAX(b.buhcurrisn) AS buhcurrisn
                               ,MAX(b.ruleisnagr) AS ruleisnagr 
                               ,MAX(b.sagroup) AS sagroup 
                               ,MAX(b.agrcurrisn) AS agrcurrisn
                               ,MAX(b.datebegcond) AS datebegcond
                               ,MAX(b.dateendcond) AS dateendcond
                               ,MAX(b.condpremagr) AS condpremagr
                               ,MAX(b.repclassisn) AS repclassisn 
                               -- { EGAO 24.06.2013
                               ,b.ruleisn 
                               ,b.subaccisn
                               ,b.parentobjclassisn
                               ,MAX(b.IsSub) AS IsSub  
                               ,MAX(b.clientjuridical) AS clientjuridical
                               ,MAX(b.clientorgformisn) AS clientorgformisn
                               ,b.rptclassisn
                               -- } EGAO 24.06.2013
                               ,MAX(b.agrclassisn) AS agrclassisn -- EGAO 19.07.2013
                        FROM ( 
                              SELECT --+ index ( b X_REPBUH2COND_AGRISN ) use_hash ( x ) use_hash ( ag ) use_hash ( ext ) no_merge ( ext ) no_merge ( ag )
                                     b.isn, deptisn, statcode, b.agrisn, bodyisn, b.condisn, rptgroupisn,
                                     b.datebeg, b.dateend,
                                     CASE
                                       WHEN TRUNC(b.datebeg)<=vDateRep THEN b.comission
                                       ELSE 0
                                     END AS comission/*EGAO 03.06.2011 письмо от Дмитревской comission*/,
                                     buhamount, buhamountrub, condpc*docsumpc*factpc*buhpc AS buhpc, isrevaluation, dateval,
                                     buhcurrisn,
                                     b.ruleisnagr,
                                     b.motivgroupisn, b.sagroup, b.agrcurrisn, b.objisn,b.riskclassisn,
                                     b.rptclass,
                                     CASE
                                       WHEN b.condisn=0 THEN trunc(b.datebeg)
                                       WHEN trunc(b.datebegcond)>trunc(b.dateend) THEN trunc(b.dateend)
                                       WHEN trunc(b.dateendcond)<trunc(b.datebeg) THEN trunc(b.datebeg)
                                       WHEN trunc(b.datebegcond)<trunc(b.datebeg) THEN trunc(b.datebeg)
                                       ELSE trunc(b.datebegcond)
                                     END AS datebegcond,
                                     CASE
                                       WHEN b.condisn=0 THEN trunc(b.dateend)
                                       WHEN trunc(b.datebegcond)>trunc(b.dateend) THEN trunc(b.dateend)
                                       WHEN trunc(b.dateendcond)<trunc(b.datebeg) THEN trunc(b.datebeg)
                                       WHEN trunc(b.dateendcond)>trunc(b.dateend) THEN trunc(b.dateend)
                                       ELSE trunc(b.dateendcond)
                                     END AS dateendcond,

                                     b.objclassisn,
                                     b.condpremagr
                                     , x.repclassisn -- EGAO 29.05.2012
                                     -- {EGAO 24.06.2013
                                     ,b.ruleisn 
                                     ,b.subaccisn
                                     ,b.parentobjclassisn 
                                     ,decode(ext.agrisn,null,0,1) AS IsSub
                                     ,ag.clientjuridical
                                     ,ag.clientorgformisn
                                     ,b.rptclassisn
                                     -- }EGAO 24.06.2013
                                     ,b.agrclassisn -- EGAO 19.07.2013
                              FROM repbuh2cond b, v_rnprepclass x,
                                   -- {EGAO 24.06.2013
                                   (
                                    SELECT --+ ordered use_nl ( ag sb ) index ( sb X_REPSUBJECT ) materialize
                                           ag.agrisn, ag.clientjuridical, sb.orgformisn AS clientorgformisn
                                    FROM repagr ag, repSubject sb
                                    WHERE ag.agrisn > vMinIsn and ag.agrisn <= vMaxIsn
                                      and ag.clientisn=sb.isn(+)
                                   ) ag,
                                   (
                                    SELECT --+ index ( ext X_AGREXT_AGR ) materialize
                                           DISTINCT ext.agrisn
                                    FROM ais.agrext ext
                                    WHERE ext.agrisn > vMinIsn and ext.agrisn <= vMaxIsn
                                      AND ext.classisn=1071774425
                                      AND ext.x1=1283168203
                                   ) ext 
                                   -- {EGAO 24.06.2013
                              WHERE b.agrisn > vMinIsn and b.agrisn<=vMaxIsn--b.isn > vMinIsn and b.Isn<=vMaxIsn
                                and b.dateval <= vDateRep --Интересуют только проводки до отчетной даты
                                and (statcode in (38,34)
                                     OR statcode = 32 AND trunc(b.datebeg)<=vDateRep
                                     or statcode = 20102 and b.agrclassisn in (AgrInFacult, AgrInRetroc, AgrInOblig) AND trunc(b.datebeg)<=vDateRep
                                     or statcode = 99
                                    )
                                and ((b.deptisn = PrivDept
                                      and b.rptgroupisn NOT IN (SELECT isn FROM dicti d START WITH isn=755103500 CONNECT by PRIOR d.isn=d.parentisn) /*EGAO 30.03.2010 в рамках 14283405403 rptgroupisn<>755103500*/
                                      and b.dateval > add_months (vDateRep,-1)
                                      and b.dateval <= vDateRep
                                      and b.datebeg>=add_months (vDateRep,-1)
                                     )
                                     or (b.deptisn = CargoDept and b.dateval > add_months (vDateRep,-12) and b.dateval <= vDateRep)
                                     or (b.deptisn = CreditDept and b.dateval > add_months (vDateRep,-6) and b.dateval <= vDateRep)
                                     or (nvl (b.rptgroupisn,0) in (755075000,755078500) and b.dateval > add_months (vDateRep,-12) and b.dateval <= vDateRep)
                                     or (b.deptisn not in (CargoDept, CreditDept)
                                         and Not (b.deptisn = PrivDept
                                                  and b.rptgroupisn NOT IN (SELECT isn FROM dicti d START WITH isn=755103500 CONNECT by PRIOR d.isn=d.parentisn) /*EGAO 30.03.2010 в рамках 14283405403  rptgroupisn<>755103500*/
                                                 )
                                         and (nvl (b.rptgroupisn,0) not in (755075000,755078500))
                                         and trunc(b.dateend) > vDateRep
                                        )
                                    )
                                AND b.sagroup IN (1, 2)--EGAO 29.05.2012 and b.sagroup = 1
                                --{EGAO 17.10.2011 изменения в ТЗ
                                AND NVL(b.ruleisnagr,0)<>747261800
                                --}
                                --{EGAO 29.05.2012
                                AND b.sagroup=x.sagroup
                                AND b.subaccisn NOT IN (1022579925, 1017809225, 1022585025)
                                --}
                                --{ EGAO 24.06.2013
                                AND ag.agrisn(+)=b.agrisn
                                AND ext.agrisn(+)=b.agrisn
                                --} EGAO 24.06.2013
                             ) b
                        GROUP BY  b.deptisn, b.agrisn, b.condisn, b.bodyIsn, b.rptgroupisn, 
                                  b.rptclass, b.riskclassisn, b.objisn, motivgroupisn, 
                                  b.objclassisn
                                  -- { EGAO 24.06.2013
                                  ,b.ruleisn 
                                  ,b.subaccisn
                                  ,b.parentobjclassisn 
                                  ,b.rptclassisn     
                                  -- } EGAO 24.06.2013
                       ) b,
                       /*(SELECT DISTINCT agrisn, repclassisn, agrpaidsum FROM ps) x1,
                       ps x2,*/
                       (SELECT --+ index ( x X_REP_LONGAGR_PAYSUM_AGR )
                            x.agrisn, 
                            x.condisn, 
                            x.repclassisn, 
                            SUM(x.paidsum) AS condpaidsum
                         FROM rep_longagr_paysum x
                         WHERE x.agrisn > pMinIsn and x.agrisn<=pMaxIsn
                           AND x.dateval<=vDateRep 
                           AND x.statcode IN (34,38)
                           AND x.loadisn=pLoadIsn
                          GROUP BY x.agrisn, x.condisn, x.repclassisn
                        ) x2,
                        (SELECT --+ index ( x X_REP_LONGAGR_PAYSUM_AGR )
                            x.agrisn, 
                            x.repclassisn, 
                            SUM(x.paidsum) AS agrpaidsum
                         FROM rep_longagr_paysum x
                         WHERE x.agrisn > pMinIsn and x.agrisn<=pMaxIsn
                           AND x.dateval<=vDateRep 
                           AND x.statcode IN (34,38)
                           AND x.loadisn=pLoadIsn
                          GROUP BY x.agrisn, x.repclassisn
                        ) x1,
                       (SELECT --+ index ( x X_LONGAGRADDENDUM_AGR)
                               agrisn, SUM(x.premiumsum) AS premiumsum
                        FROM rep_longagraddendum x
                        WHERE x.agrisn > pMinIsn and x.agrisn<=pMaxIsn
                          AND least(nvl(x.datebeg,x.datesign),nvl(x.datesign, x.datebeg))<=vDateRep
                        GROUP BY x.agrisn  
                       ) x3,
                       (
                        SELECT --+ index ( oa X_ATTRIB_OBJ_CLASS )
                               oa.objisn AS agrisn, COUNT(1) AS istender
                        FROM ais.obj_attrib oa
                        WHERE oa.objisn > pMinIsn and oa.objisn<=pMaxIsn
                          AND oa.Discr = 'A'  -- договор
                          AND oa.classisn=3999680403
                          and val='1'
                        GROUP BY oa.objisn  
                       ) tdr
                  WHERE x2.agrisn(+)=b.agrisn
                    AND x2.condisn(+)=b.condisn
                    AND x2.repclassisn(+)=b.repclassisn
                    AND x1.agrisn(+)=b.agrisn
                    AND x1.repclassisn(+)=b.repclassisn
                    AND x3.agrisn(+)=b.agrisn
                    AND tdr.agrisn(+)=b.agrisn
              ) b
          ) b
        ;




    ----Всякие 1/24 , 1/8 и т.д.
    update rnp_rsbu_buffer a
    set k_rnp_rsbucond=decode (condformulaisn,
                               1, greatest (0,(3.5-trunc (months_between (vDateRep, trunc (DateVal,'MM'))/3))/4 ),
                               2, (Case When deptisn = PrivDept and conddatebeg>vDateRep then 1 Else 0.5 end),
                               3, greatest (0,(1.5-trunc (months_between (vDateRep, trunc (DateVal,'MM'))/3) ) /2),
                               4, 1-(months_between (vDateRep, trunc (DateVal,'MM')-1) -0.5)/12,
                               5, 1-(months_between (vDateRep, trunc (DateVal,'MM')-1)-0.5)/3
                              )
    where condformulaisn between 1 and 5
      and dateval between add_months (vDateRep,-decode (condformulaisn,1,12,2,1,3,6,4,12,5,3))+1 and vDateRep;

    update rnp_rsbu_buffer
    set rnpshare = decode (agrformulaisn,
      1, greatest (0,(3.5-trunc (months_between (vDateRep, trunc (DateVal,'MM'))/3))/4 ),
      2, (Case When deptisn = PrivDept and Datebeg>vDateRep then 1 Else 0.5 end),
      3, greatest (0,(1.5-trunc (months_between (vDateRep, trunc (DateVal,'MM'))/3) ) /2),
      4, 1-(months_between (vDateRep, trunc (DateVal,'MM')-1) -0.5)/12,
      5, 1-(months_between (vDateRep, trunc (DateVal,'MM')-1)-0.5)/3)
    where agrformulaisn between 1 and 5
      and dateval between add_months (vDateRep,-decode (agrformulaisn,1,12,2,1,3,6,4,12,5,3))+1 and vDateRep;


    -----Подгоняем дату окончания для длинных договоров
    update rnp_rsbu_buffer
    set dateend = least (dateend,add_months (datebeg,trunc (months_between (vDateRep,datebeg+1)/12)*12+12)-1)
    where agrformulaisn = 7
      and datebeg <= vDateRep
      and dateend > vDateRep;

    -----Классическая Pro Rata
    update rnp_rsbu_buffer
    set k_rnp_rsbucond = decode (sign (conddatebeg-vDateRep),1,1,(conddateend-vDateRep)/(conddateend-conddatebeg+1))
    where condformulaisn=6
      and (conddatebeg > vDateRep or conddateend > vDateRep);

    update rnp_rsbu_buffer
    set rnpshare = decode (sign (datebeg-vDateRep),1,1,(dateend-vDateRep)/(dateend-datebeg+1))
    where agrformulaisn in (6,7)
      and (datebeg > vDateRep or dateend > vDateRep);
    -----Хитрая Pro Rata для длинных договоров
    update rnp_rsbu_buffer b
    set k_rnp_rsbucond = decode (sign (conddatebeg-vDateRep),1,1, decode(sign(b.condpremiumsum),1,
    greatest (least (b.condnachprem/b.condpremiumsum,1)-(vDateRep-condDateBeg+1)/(condDateEnd-condDateBeg+1),0)/
    least (b.condnachprem/b.condpremiumsum,1),(b.conddateend-vDateRep)/(b.conddateend-b.conddatebeg+1)))
    where condformulaisn = 7
      and (conddatebeg > vDateRep or conddateend > vDateRep)
      and 0 < b.condnachprem;

    update rnp_rsbu_buffer b
    set rnpshare = decode (sign (datebeg-vDateRep),1,1, decode(sign(b.agrpremiumsum),1,

    greatest (least (b.agrnachprem/b.agrpremiumsum,1)-(vDateRep-DateBeg+1)/(DateEnd-DateBeg+1),0)/
    least (b.agrnachprem/b.agrpremiumsum,1),(b.dateend-vDateRep)/(b.dateend-b.datebeg+1)))

    where agrformulaisn = 8
      and (datebeg > vDateRep or dateend > vDateRep)
      and 0 < b.agrnachprem;

    --{EGAO 06.03.2012
    /*UPDATE
    (SELECT a.rnpshare, b.rnpshare AS val
     FROM rnp_rsbu_buffer a,
          rep_rnposagoshortagr b -- !!! заполняется в пакете report_rnp
     WHERE a.rptgroupisn=818752900 -- ОСАГО
       AND b.agrisn=a.agrisn
    ) SET rnpshare=val;*/
    UPDATE rnp_rsbu_buffer a 
    SET a.rnpshare=(SELECT --+ index ( b X_RNPOSAGOSHORTAGR_AGRISN )
                           b.rnpshare 
                    FROM rnposagoshortagr b WHERE b.loadisn=pLoadIsn AND b.agrisn=a.agrisn)
    WHERE a.rptgroupisn=818752900 -- ОСАГО
      AND EXISTS (SELECT 'x' FROM rnposagoshortagr b WHERE b.loadisn=pLoadIsn AND b.agrisn=a.agrisn)
    ;
    
    --}


    -----Заполняем курсы
    update rnp_rsbu_buffer
    set rate = decode (currisn,EuroCurr,vEuroRate,DollarCurr,vDollarRate,GetCrossCover (1,currisn,LocalCurr,vDateRep))
    where isrevaluation = 1
    --AND buhcondisn > vMinIsn and buhcondisn<=vMaxIsn
    ;
    -----Инсертим премию и входящую комиссию
    insert into rnp_rsbu (isn, buhcondisn, deptisn, agrisn, bodyisn, condisn, daterep, dateval, rptgroupisn, loadisn,
      bruttosum, k_rnp_rsbuagr, bruttopc, nettopc, classisn, currisn,agrformulaisn,ruleisnagr,isrevaluation,status,budgetgroupisn,sagroup,
      AGRCURRISN,objisn, riskclassisn, rptclass, datebegcond, dateendcond, datebeg, dateend, k_rnp_rsbucond, condformulaisn, objclassisn
      , agrnachprem, agrpremiumsum, condnachprem, condpremiumsum
      , repclassisn -- EGAO 29.05.2012
      -- {EGAO 24.06.2013
      ,ruleisn 
      ,subaccisn
      ,parentobjclassisn
      ,IsSub
      ,clientjuridical 
      ,clientorgformisn 
      ,rptclassisn
      -- }EGAO 24.06.2013
      -- {EGAO 19.07.2013
      ,agrclassisn 
      ,istender
      -- }EGAO 19.07.2013
      )
    select
      Seq_Reports.NextVal, buhcondisn, deptisn, agrisn, bodyisn, condisn, vDateRep, dateval, rptgroupisn, pLoadIsn,
      buhpc*decode (isrevaluation,1,buhamount*rate,buhamountrub), rnpshare, 1, 1, decode (statcode,20102,32,statcode),
      currisn,agrformulaisn,RULEISNAGR,ISREVALUATION,'Y',BUDGETGROUPISN,SaGROUP,
      AGRCURRISN,objisn, riskclassisn, rptclass, conddatebeg, conddateend, datebeg, dateend, k_rnp_rsbucond, condformulaisn, objclassisn
      , agrnachprem, agrpremiumsum, condnachprem, condpremiumsum
      , repclassisn -- EGAO 29.05.2012
      -- {EGAO 24.06.2013
      ,ruleisn 
      ,subaccisn
      ,parentobjclassisn
      ,IsSub
      ,clientjuridical 
      ,clientorgformisn 
      ,rptclassisn 
      -- }EGAO 24.06.2013
      -- {EGAO 19.07.2013
      ,agrclassisn 
      ,istender
      -- }EGAO 19.07.2013
    from rnp_rsbu_buffer
    where statcode in (38,34,32,20102)
      and (rnpshare > 0 OR k_rnp_rsbucond > 0) -- EGAO 26.01.2011 rnpshare > 0
      ;

    -----Инсертим прямую комиссию
    insert into rnp_rsbu (isn, buhcondisn, deptisn, agrisn, bodyisn, condisn, daterep, dateval, rptgroupisn, loadisn,
      bruttosum, k_rnp_rsbuagr, bruttopc, nettopc, classisn, currisn,agrformulaisn,RULEISNAGR,ISREVALUATION,Status,
      BUDGETGROUPISN,SaGROUP, AGRCURRISN, objisn, riskclassisn, rptclass, datebegcond, dateendcond, datebeg, dateend, k_rnp_rsbucond, condformulaisn, objclassisn
      , agrnachprem, agrpremiumsum, condnachprem, condpremiumsum
      , repclassisn -- EGAO 29.05.2012
      -- {EGAO 24.06.2013
      ,ruleisn 
      ,subaccisn
      ,parentobjclassisn
      ,IsSub
      ,clientjuridical 
      ,clientorgformisn 
      ,rptclassisn
      -- }EGAO 24.06.2013
      -- {EGAO 19.07.2013
      ,agrclassisn 
      ,istender
      -- }EGAO 19.07.2013
      )
    select
      Seq_Reports.NextVal, buhcondisn, deptisn, agrisn, bodyisn, condisn, vDateRep, dateval, rptgroupisn, pLoadIsn,
      -buhpc*decode (isrevaluation,1,buhamount*rate,buhamountrub), rnpshare, comission/100, 1, 20102,
      currisn,agrformulaisn,RULEISNAGR,ISREVALUATION,'Y',
      BUDGETGROUPISN,SaGROUP, AGRCURRISN, objisn, riskclassisn, rptclass, conddatebeg, conddateend, datebeg, dateend, k_rnp_rsbucond, condformulaisn, objclassisn
      , agrnachprem, agrpremiumsum, condnachprem, condpremiumsum
      , repclassisn -- EGAO 29.05.2012
      -- {EGAO 24.06.2013
      ,ruleisn 
      ,subaccisn
      ,parentobjclassisn
      ,IsSub
      ,clientjuridical 
      ,clientorgformisn 
      ,rptclassisn 
      -- }EGAO 24.06.2013
      -- {EGAO 19.07.2013
      ,agrclassisn 
      ,istender
      -- }EGAO 19.07.2013
    from rnp_rsbu_buffer
    where statcode in (38)
      and (rnpshare > 0 OR k_rnp_rsbucond > 0) -- EGAO 26.01.2011 rnpshare > 0
      and comission > 0
      ;

    -----Инсертим вычет по ОСАГО и "Обязательное страхование опо"
    insert into rnp_rsbu (isn, buhcondisn, deptisn, agrisn, bodyisn, condisn, daterep, dateval, rptgroupisn, loadisn,
      bruttosum, k_rnp_rsbuagr, bruttopc, nettopc, classisn, currisn,agrformulaisn,RULEISNAGR,
      ISREVALUATION,Status,BUDGETGROUPISN,SaGROUP, AGRCURRISN, objisn, riskclassisn, rptclass,
      datebegcond, dateendcond, datebeg, dateend, k_rnp_rsbucond, condformulaisn, objclassisn
      , agrnachprem, agrpremiumsum, condnachprem, condpremiumsum
      , repclassisn -- EGAO 29.05.2012
      -- {EGAO 24.06.2013
      ,ruleisn 
      ,subaccisn
      ,parentobjclassisn
      ,IsSub
      ,clientjuridical 
      ,clientorgformisn 
      ,rptclassisn
      -- }EGAO 24.06.2013
      -- {EGAO 19.07.2013
      ,agrclassisn
      ,istender
      -- }EGAO 19.07.2013
      )
    select
      Seq_Reports.NextVal, buhcondisn, deptisn, agrisn, bodyisn, condisn, vDateRep, dateval, rptgroupisn, pLoadIsn,
      -buhpc*decode (isrevaluation,1,buhamount*rate,buhamountrub), rnpshare, 0.03, 1, 1,
      currisn,agrformulaisn,RULEISNAGR,ISREVALUATION,'Y',BUDGETGROUPISN,SaGROUP, AGRCURRISN,
      objisn, riskclassisn, rptclass, conddatebeg, conddateend, datebeg, dateend, k_rnp_rsbucond, condformulaisn, objclassisn
      , agrnachprem, agrpremiumsum, condnachprem, condpremiumsum
      , repclassisn -- EGAO 29.05.2012
      -- {EGAO 24.06.2013
      ,ruleisn 
      ,subaccisn
      ,parentobjclassisn
      ,IsSub
      ,clientjuridical 
      ,clientorgformisn 
      ,rptclassisn 
      -- }EGAO 24.06.2013
      -- {EGAO 19.07.2013
      ,agrclassisn 
      ,istender
      -- }EGAO 19.07.2013
    from rnp_rsbu_buffer
    where statcode  in (38)
      and (rnpshare > 0 OR k_rnp_rsbucond > 0) -- EGAO 26.01.2011 rnpshare > 0
      and rptgroupisn IN (818752900, 3540195803, 4138698903)-- EGAO 01.04.2013 rptgroupisn IN (818752900, 3540195803)-- EGAO 04.04.2012 rptgroupisn = 818752900
      ;

    DELETE FROM rnp_rsbu_buffer;

    COMMIT;

  END;


  PROCEDURE make_rnp_rsbu_r_by_isn(pLoadisn IN NUMBER,pMinIsn IN NUMBER, pMaxIsn IN NUMBER)
  IS
    vDateRep DATE := GetDateRep(pLoadIsn);

    TYPE TRecord IS RECORD(
      agrisn NUMBER,
      loadisn number,
      cnt INTEGER,
      rn INTEGER
    );
    TYPE TTab IS TABLE OF TRecord INDEX BY BINARY_INTEGER;
    Tab TTab;
    i BINARY_INTEGER;

    CURSOR  a(curMinIsn NUMBER, curMaxIsn NUMBER, curLoadIsn NUMBER)
    IS
    select x.*, count(1) over () as cnt, rownum rn
    from (
    SELECT --+ index ( x X_RNP_RSBU_AGRISN )
           x.agrisn, max(x.loadisn) as loadisn
    FROM rnp_rsbu x
    WHERE x.agrisn> curMinIsn AND x.agrisn <= curMaxIsn
      and x.loadisn=curLoadIsn
      AND x.isdar IS NULL -- EGAO 17.12.2013
    GROUP BY x.agrisn) x;

  BEGIN
    IF vDateRep IS NULL THEN
      raise_application_error(en_invalid_daterep,'Invalid daterep');
    END IF;
    OPEN a(pMinIsn, pMaxIsn, pLoadIsn);
    LOOP
       FETCH a BULK COLLECT INTO Tab LIMIT 100;
       EXIT WHEN Tab.count=0;
       FOR i in Tab.FIRST..Tab.LAST LOOP

         dbms_application_info.set_module('rnp_rsbu_r',Tab(i).rn ||' of '||Tab(i).cnt);

         INSERT INTO storages.rnp_rsbu_r(
           loadisn, daterep, agrisn, condisn, deptisn, isrevaluation, rptclass, rptgroupisn, k_rnp_rsbuagr,
           k_rnp_rsbucond, rnp_rsbucond, rnp_rsbuagr, sagroup, conddatebeg, conddateend, agrid, agrdatebeg,
           agrdateend, agrclassisn, agrcurrisn, agrcomission, agrruleisn, objisn, riskclassisn, riskisn, premamountrub
           , repclassisn -- EGAO 29.05.2012
           )
         SELECT --+ no_merge( r ) ordered use_nl ( r rc ra )
                     r.loadisn,
                     r.daterep,
                     r.agrisn,
                     r.condisn,
                     r.deptisn,
                     r.isrevaluation,
                     r.rptclass,
                     r.rptgroupisn,
                     r.k_rnp_rsbuagr,
                     r.k_rnp_rsbucond,
                     r.rnp_rsbucond,
                     r.rnp_rsbuagr,
                     r.sagroup,
                     trunc(rc.datebeg) as conddatebeg,
                     trunc(rc.dateend) as conddateend,
                     ra.id AS agrid,
                     trunc(ra.datebeg) AS agrdatebeg,
                     trunc(ra.dateend) AS agrdateend,
                     ra.classisn AS agrclassisn,
                     ra.currisn AS agrcurrisn,
                     ra.comission AS agrcomission,
                     ra.ruleisn AS agrruleisn,
                     r.objisn,
                     r.riskclassisn,
                     rc.riskisn, 
                     r.premamountrub
                     , r.repclassisn -- EGAO 29.05.2012
         FROM (SELECT --+ index( r X_RNP_RSBU_AGRISN )
                     r.loadisn,
                     MAX(r.daterep) AS daterep,
                     r.agrisn,
                     r.condisn,
                     r.deptisn,
                     r.isrevaluation,
                     r.rptclass,
                     r.rptgroupisn,
                     MAX(r.k_rnp_rsbuagr) AS k_rnp_rsbuagr,
                     MAX(r.k_rnp_rsbucond) AS k_rnp_rsbucond,
                     SUM(r.bruttosum*r.bruttopc*r.k_rnp_rsbucond) AS rnp_rsbucond,
                     SUM(r.bruttosum*r.bruttopc*r.k_rnp_rsbuagr) as rnp_rsbuagr,
                     r.sagroup,
                     MAX(r.objisn) AS objisn,
                     MAX(r.riskclassisn) AS riskclassisn,
                     SUM(CASE WHEN r.classisn IN (34,38) THEN r.bruttosum END)AS premamountrub
                     , r.repclassisn -- EGAO 29.05.2012
                FROM storages.rnp_rsbu r
                WHERE r.loadisn=Tab(i).loadisn
                  AND r.agrisn=Tab(i).agrisn
                  AND r.isdar IS NULL -- EGAO 17.12.2013
                GROUP BY r.loadisn, r.agrisn, r.condisn, r.deptisn, r.isrevaluation, r.rptclass, r.rptgroupisn, r.sagroup
                         , r.repclassisn -- EGAO 29.05.2012
               ) r, repcond rc, repagr ra
         WHERE rc.condisn(+)=r.condisn
           AND ra.agrisn(+)=r.agrisn;
       END LOOP;
       COMMIT;
    END LOOP;
    COMMIT;
    IF a%ISOPEN THEN
      CLOSE a;
    END IF;
  END;
  
  PROCEDURE make_resrnpsummary(ploadisn IN NUMBER) -- EGAO 14.06.2013
  IS
    vLoadIsn NUMBER := nvl(pLoadIsn, GetActiveLoad(trunc(SYSDATE,'mm')-1));
    vDateRep DATE := GetDateRep(vLoadIsn);
    vLoadObjCnt number:=50000;
  BEGIN  
    IF vLoadIsn IS NULL  THEN
      raise_application_error(en_invalid_loadisn,'Invalid loadisn');
    END IF;
    IF vDateRep IS NULL  THEN
      raise_application_error(en_invalid_daterep,'Invalid daterep');
    END IF;

    replog_i (vLoadIsn, 'LoadRNPNEW', 'MakeRnpRsbuSummary',pAction=>'Begin');
    LOOP 
      DELETE FROM resrnpsummary a 
      WHERE a.LoadIsn=vLoadIsn and rownum<=vLoadObjCnt;
      EXIT WHEN sql%ROWCOUNT=0;
      COMMIT;
    END LOOP;
    
    
    INSERT INTO resrnpsummary (isn, 
                               loadisn, 
                               daterep, 
                               dateval, 
                               deptisn, 
                               rptgroupisn, 
                               classisn, 
                               currisn, 
                               bruttosum, 
                               bruttornp,
                               repclass)
    SELECT Seq_Reports.NEXTVAL, vLoadIsn, S.daterep, S.dateval,
           S.deptisn,S.rptgroupisn, S.Classisn, S.CurrIsn,
           S.bruttosum, S.bruttornp,sagroup
    FROM ( SELECT --+ parallel ( r 32 )  full ( r )
                   r.daterep,
                   r.dateval,
                   r.deptisn,
                   r.rptgroupisn,
                   r.rptclass,
                   r.classisn, r.currisn, r.sagroup,
                   SUM(r.bruttosum*r.bruttopc) AS bruttosum,
                   SUM(r.bruttosum*r.bruttopc*r.k_rnp_rsbucond) AS bruttornp
           FROM storages.rnp_rsbu r
           WHERE r.loadisn=vLoadisn
             AND NVL(r.repclassisn,1)=1
             AND r.isdar IS NULL -- EGAO 17.12.2013
           GROUP BY r.daterep, r.dateval,r.deptisn, r.rptgroupisn, r.rptclass, r.classisn, r.currisn, r.sagroup
           
         ) S;
    COMMIT;
    replog_i (vLoadIsn, 'LoadRNPNEW', 'MakeRnpRsbuSummary',pAction=>'End');
  END;  

  PROCEDURE make_rnp_re_rsbu(pLoadIsn NUMBER := NULL)
  IS
    vLoadIsn NUMBER := nvl(pLoadIsn, GetActiveLoad(trunc(SYSDATE,'mm')-1));
    vDateRep DATE := GetDateRep(vLoadIsn);
    vMinIsn number:=-1;
    vMaxIsn number;
    vSql varchar2(4000);
    SesId Number;
    vLoadObjCnt number:=50000;
    vCnt number:=0;
    vPart VARCHAR2(30);
  BEGIN
    IF vLoadIsn IS NULL  THEN
      raise_application_error(en_invalid_loadisn,'Invalid loadisn');
    END IF;
    IF vDateRep IS NULL  THEN
      raise_application_error(en_invalid_daterep,'Invalid daterep');
    END IF;

    replog_i (vLoadIsn, 'LoadRNPNEW', 'MakeRnpReRsbu',pAction=>'Begin');

    vPart :=  INIT_PARTITION_BY_KEY(pTableName => 'storages.rnp_re_rsbu', pKey => vLoadIsn, pCompress => 1);
    --!!!
    Execute Immediate 'ALTER TABLE storages.rnp_re_rsbu MODIFY PARTITION '||vPart||' UNUSABLE LOCAL INDEXES';
    --!!!
    dbms_lock.sleep(20);

    SesId:=Parallel_Tasks.createnewsession();
    vMinIsn:=-1;


    LOOP

      vMaxIsn:=Cut_Table('storage_source.repagr','agrisn',vMinIsn,pRowCount=>vLoadObjCnt);

      EXIT WHEN vMaxIsn IS NULL;



      vSql:= 'DECLARE
                vMinIsn number :='||vMinIsn||';
                vMaxIsn number :='||vMaxIsn||';
                vCnt    number :='||vCnt||';
                vLoadIsn number := '||vLoadIsn||';
              BEGIN
                DBMS_APPLICATION_INFO.SET_MODULE(''rnp_re_rsbu'',''Precess#''||vCNT);
                storages.report_rnp_new.make_rnp_re_rsbu_by_isn(vLoadIsn, vMinIsn, vMaxIsn);
             END;';

      System.Parallel_Tasks.processtask(sesid,vsql);

      vCnt:=vCnt+1;

      vMinIsn:=vMaxIsn;
      DBMS_APPLICATION_INFO.set_module('rnp. fill rnp_re_rsbu','Applied: '||vCnt*vLoadObjCnt);

    END LOOP;

    -- ждем, пока завершатся все джобы
    Parallel_Tasks.endsession(sesid);

    REBULD_TABLE_INDEX('storages.rnp_re_rsbu', PPARTITITON=>vPart);
    --make_rnp_re_rsbu_6654(vloadisn, vDateRep);
    --make_rnp_re_rsbu_6693(vloadisn, vDateRep);

    replog_i (vLoadIsn, 'LoadRNPNEW', 'MakeRnpReRsbu',pAction=>'End');
  END;


  PROCEDURE make_rnp_re_rsbu_by_isn(pLoadIsn NUMBER, pMinIsn Number,pMaxIsn Number)
  IS

    vDateRep DATE := GetDateRep(pLoadIsn);
    vCondIsn NUMBER := -999;
    vAgrIsn NUMBER := -999;
    vSectIsn NUMBER := -999;
    vReRNP NUMBER;
    vReRNP_t NUMBER;
    i NUMBER;
    ContRNPPrevAmount NUMBER;



    vPrioritySum NUMBER := 0;
    vLayerShare NUMBER := 0;
    vIndex BINARY_INTEGER;

    TYPE TCond IS TABLE OF v_rnp_re_rsbu%ROWTYPE;
    CondTab TCond;


    CURSOR cond2sect_cur(agrisn_in NUMBER, condisn_in NUMBER, secttype_in IN VARCHAR2, agrdatebeg_in DATE, conddatebeg_in DATE)
    IS
    SELECT MAX(s.secttype) AS secttype,
           MAX(s.datebeg) AS datebeg,
           MAX(s.currisn) AS currisn,
           NVL(MIN(cd.prioritysum),0) AS prioritysum,
           NVL(SUM(CASE WHEN pr.parentisn IS NULL THEN pr.sharepc END)/100,0) AS sharepc
    FROM (SELECT s.*,
                 rank() over (ORDER BY CASE secttype_in
                                         WHEN 'RX' THEN CASE s.secttype WHEN 'QS' THEN 1 WHEN 'XL' THEN 2 END
                                         WHEN 'QS' THEN CASE s.secttype WHEN 'RX' THEN 1 WHEN 'QS' THEN 2 WHEN 'XL' THEN 3  END
                                         WHEN 'SP' THEN CASE s.secttype WHEN 'RX' THEN 1 WHEN 'QS' THEN 2 WHEN 'XL' THEN 3  END
                                       END) AS rn
          FROM (SELECT --+ ordered use_nl ( x reagr ) index ( x X_REP_AGRRE_SECTISN ) 
                       x.sectisn,
                       x.secttype,
                       x.sectcurrisn AS currisn,
                       x.sdatebeg AS datebeg,
                       COUNT(CASE WHEN x.secttype='QS' THEN 1 END) over  (PARTITION BY x.secttype) AS qscnt,
                       COUNT(CASE WHEN x.secttype='XL' THEN 1 END) over  (PARTITION BY x.secttype) AS xlcnt,
                       COUNT(CASE WHEN x.secttype='RX' THEN 1 END) over  (PARTITION BY x.secttype) AS rxcnt
                 FROM rep_agrre x,
                      repagr reagr
                 WHERE x.agrisn=agrisn_in
                   AND (x.condisn IS NULL OR x.condisn=condisn_in)
                   AND reagr.agrisn=x.reisn
                   AND CASE
                         WHEN reagr.datebase='C' AND vDateRep BETWEEN trunc(x.sdatebeg) AND trunc(x.sdateend)  THEN 1
                         WHEN reagr.datebase='I' AND (agrdatebeg_in BETWEEN trunc(x.sdatebeg) AND trunc(x.sdateend) OR conddatebeg_in BETWEEN trunc(x.sdatebeg) AND trunc(x.sdateend)) THEN 1
                         ELSE 0
                       END = 1
                   --{EGAO 24.08.2011
                   AND CASE secttype_in
                         WHEN 'RX' THEN CASE WHEN reagr.classisn=9018 AND x.secttype IN ('QS','XL') THEN 1 ELSE 0 END
                         WHEN 'QS' THEN CASE WHEN reagr.classisn=9058 AND x.secttype='RX' OR reagr.classisn=9018 AND x.secttype IN ('QS', 'XL') THEN 1 ELSE 0 END
                         WHEN 'SP' THEN CASE WHEN reagr.classisn=9058 AND x.secttype='RX' OR reagr.classisn=9018 AND x.secttype IN ('QS', 'XL') THEN 1 ELSE 0 END
                         ELSE 0
                       END = 1
                   --}

                ) s
          WHERE  qscnt>0 OR xlcnt=1 OR rxcnt>0 -- EGAO 17.05.2011 WHERE  (s.secttype='QS' AND qscnt>0) OR (s.secttype='XL' AND xlcnt=1) OR (rxcnt>0)
         ) s,
         recond cd,
         resubjperiod pr
    WHERE cd.sectisn=s.sectisn AND pr.condisn=cd.isn AND rn=1
      AND cd.isn NOT IN (119901171303, 111824735603) -- EGAO 11.01.2012 Лейеры заведены некорректно, исправлению в АИС не подлежат. Исключаем их обработку в коде
    ;

    cond2sect_rec cond2sect_cur%ROWTYPE;

    CURSOR layer_cur(sectisn_in NUMBER, redatebase_in VARCHAR2, agrdatebeg_in DATE, conddatebeg_in DATE, sectdatebeg_in DATE, sectdateend_in DATE)
    IS
    --{EGAO 14.08.2013 в рамках ДИТ-13-3-205032
    /*SELECT cd.isn,
           MAX(decode(cd.rate,NULL,1,0)) AS rateisnull,
           MAX(decode(cd.limitsum,NULL,1,0)) AS limitsumisnull,
           NVL(MAX(cd.rate)/100,0) AS rate,
           NVL(MAX(cd.limitsum),0) AS limitsum,
           NVL(MAX(cd.prioritysum),0) AS prioritysum,
           NVL(max(cd.depospremsum),0) AS depospremsum,
           NVL(SUM(CASE WHEN pr.parentisn IS NULL THEN pr.sharepc END)/100,0) AS sharepc
    FROM recond cd,
         resubjperiod pr
    WHERE cd.sectisn=sectisn_in
      AND pr.condisn=cd.isn
      AND cd.isn NOT IN (119901171303, 111824735603) -- EGAO 11.01.2012 Лейеры заведены некорректно, исправлению в АИС не подлежат. Исключаем их обработку в коде
    GROUP BY cd.isn;*/
    --} EGAO 14.08.2013

    SELECT condisn, 
           MAX(decode(a.rate,NULL,1,0)) AS rateisnull,
           MAX(decode(a.limitsum,NULL,1,0)) AS limitsumisnull,
           NVL(MAX(a.rate)/100,0) AS rate,
           NVL(MAX(a.limitsum),0) AS limitsum,
           NVL(MAX(a.prioritysum),0) AS prioritysum,
           NVL(max(a.depospremsum),0) AS depospremsum,
           SUM(CASE WHEN NVL(a.condsharepc,0)=0 THEN 0 ELSE a.sharepc*a.parentsharepc/a.condsharepc END)/100 AS sharepc
    FROM (
           SELECT --+ ordered use_nl ( a cd )
                  a.condisn,
                  a.parentsharepc,
                  a.sharepc,
                  SUM(a.sharepc) over (PARTITION BY a.condisn, a.parentisn) AS condsharepc,
                  cd.rate, -- Премия лейера (в recond в %, здесь - доля)
                  cd.prioritysum, -- Приоритет лейера
                  cd.limitsum, -- Лимит лейера
                  cd.depospremsum-- Депозитная премия лейера
           FROM (
                 SELECT a.sectisn, -- секция
                        a.condisn, -- лейер
                        a.sharepc, -- доля размещения участника
                        connect_by_root(a.sharepc) AS parentsharepc, -- доля размещения брокера
                        connect_by_root(a.isn) AS parentisn
                 FROM (SELECT a.sectisn, a.condisn, a.parentisn, a.isn, a.subjisn,
                              CASE
                                WHEN redatebase_in='C'
                                     AND (a.dateentry>vDateRep  
                                          OR a.dateswitch<=vDateRep
                                          OR a.datebeg>vDateRep 
                                          OR a.dateend<vDateRep
                                         ) THEN 0
                                 WHEN redatebase_in='I'
                                     AND (a.dateentry>vDateRep
                                          OR a.dateswitch<=vDateRep
                                          OR ((conddatebeg_in BETWEEN sectdatebeg_in AND sectdateend_in AND a.datebeg>conddatebeg_in)
                                              OR 
                                              (agrdatebeg_in BETWEEN sectdatebeg_in AND sectdateend_in AND a.datebeg>agrdatebeg_in)
                                             )
                                          OR ((conddatebeg_in BETWEEN sectdatebeg_in AND sectdateend_in AND a.dateend<conddatebeg_in)
                                              OR 
                                              (agrdatebeg_in BETWEEN sectdatebeg_in AND sectdateend_in AND a.dateend<agrdatebeg_in)
                                             )) THEN 0
                                ELSE nvl(a.sharepc,0)
                              END AS sharepc 
                       FROM v_resubjperiod a
                       WHERE a.sectisn=sectisn_in 
                      ) a 
                 WHERE CONNECT_BY_ISLEAF=1 AND a.sharepc<>0
                 CONNECT BY PRIOR a.isn=a.parentisn
                 START WITH a.parentisn IS NULL AND a.sharepc<>0
                ) a,
                recond cd
           WHERE cd.isn=a.condisn
             AND cd.isn NOT IN (119901171303, 111824735603) -- EGAO 11.01.2012 Лейеры заведены некорректно, исправлению в АИС не подлежат. Исключаем их обработку в коде
         ) a
    GROUP BY a.condisn;
    
    layer_rec layer_cur%ROWTYPE;

    TYPE TSectRec IS RECORD(
      Isn NUMBER,
      Duration NUMBER, -- продолжительность в днях
      Remainder NUMBER, -- остаток действия в днях после отчетной даты
      ProRata NUMBER, -- отношение Remainder/Duration
      BruttoRNP NUMBER, -- суммарный РНП по всем перестрахованным условиям
      Isrevaluation NUMBER, -- признак переоценки
      RNPShare NUMBER,
      RNPShare_t NUMBER,
      RNPPrevAmount NUMBER,
      NachPrem NUMBER, -- суммарная начисленная премия по всем перестрахованным условиям
      EmptyRateLayerCnt NUMBER, -- кол-во лейеров секции, у которых не указана ставка перерасчета
      EPI NUMBER, -- максимальный ожидаемый объем начисленной премии среди всех лейеров
      ReNachPrem NUMBER -- начисленная исходящая премия
    );
    SectRec TSectRec;

    TYPE TSectTab IS TABLE OF TSectRec INDEX BY VARCHAR2(100);
    SectTab TSectTab;


  BEGIN
    IF vDateRep IS NULL  THEN
      raise_application_error(en_invalid_daterep,'Invalid daterep');
    END IF;

    i := gcc2.GCC2(1,LocalCurr,DollarCurr,SYSDATE);

    pparam.Clear;
    pparam.SetParamN('MinAgrIsn', pMinIsn);
    pparam.SetParamN('MaxAgrIsn', pMaxIsn);
    pparam.SetParamN('LoadIsn', pLoadIsn);

    SELECT a.*
    BULK COLLECT INTO CondTab
    FROM v_rnp_re_rsbu a
    ORDER BY a.agrisn, a.condisn, a.sectpriority, a.sectisn;


    IF CondTab.count=0 THEN
      RETURN;
    END IF;

    FOR indx IN CondTab.first..CondTab.last  LOOP
      dbms_application_info.set_module('rnp_re_rsbu ',CondTab(indx).agrisn||'-'||CondTab(indx).sectisn);
      vReRNP := 0;
      vReRNP_t := 0;

      IF CondTab(indx).agrisn<>vAgrIsn OR
         CondTab(indx).condisn<>vCondIsn OR
         CondTab(indx).sectisn<>vSectIsn THEN

        IF CondTab(indx).agrisn<>vAgrIsn OR
           CondTab(indx).condisn<>vCondIsn THEN
          --{EGAO 06.09.2013
          /*DELETE FROM \*+ index ( a X_TT_RNP_RE_RSBU_PREVSECTAM )*\tt_rnp_re_rsbu_prevsectamount a
          WHERE a.condisn=CondTab(indx).condisn AND a.agrisn=CondTab(indx).agrisn;*/
          
          DELETE --+ full ( a )
          FROM tt_rnp_re_rsbu_prevsectamount a;
          --}EGAO 06.09.2013
        END IF;

        SectRec := NULL;
        SectRec.RNPShare := 0;
        SectRec.RNPShare_t := 0;
        SectRec.RNPPrevAmount := 0;

        IF NOT SectTab.exists(to_char(CondTab(indx).sectisn)) THEN
          IF CondTab(indx).reclassisn = AgrFacultType THEN
            IF CondTab(indx).agrruleisn=CosmicRuleAgr THEN -- EGAO 14.03.2013
              SELECT --+ index (a X_REPBUHRE2RESECTION_SECT) use_hash ( a h )
                      NVL(-SUM(CASE 
                                 WHEN a.buhcurrisn=CondTab(indx).sectcurrisn THEN a.amount
                                 ELSE gcc2.gcc2(a.amount,a.buhcurrisn,CondTab(indx).sectcurrisn, a.dateval)
                               END),0)  
              INTO SectRec.ReNachPrem
              FROM repbuhre2resection_new a /*EGAO 18.12.2013 repbuhre2resection a*/, buhsubacc h
              WHERE a.sectisn=CondTab(indx).sectisn
                AND h.ISN=a.subaccisn
                AND (h.ID LIKE '924%' OR h.id LIKE '914%' OR h.id LIKE '916%')
                AND a.dateval<=vDateRep
                AND a.dsclassisn=414
                AND nvl(a.dsclassisn2,0)<>2265208403;
              
              SELECT NVL(SUM(CASE 
                               WHEN bc.buhcurrisn=CondTab(indx).sectcurrisn THEN amount
                               ELSE
                                 CASE CondTab(indx).sectcurrisn 
                                   WHEN DollarCurr THEN bc.amountusd 
                                   WHEN LocalCurr THEN bc.amountrub 
                                   ELSE gcc2.gcc2(bc.amount,bc.buhcurrisn,CondTab(indx).sectcurrisn,bc.dateval) 
                                 END
                             END),0)
              INTO SectRec.NachPrem
              FROM (
                    WITH x AS (SELECT DISTINCT
                                      x.agrisn,
                                      x.condisn
                               FROM (SELECT x.*,
                                            MIN(x.condrank) over (PARTITION BY agrisn) AS condrank2agr
                                     FROM (SELECT --+ index ( x X_REP_AGRRE_SECTISN )
                                                  x.agrisn, x.condisn,
                                                  CASE WHEN x.condisn IS NULL THEN 1 ELSE 2 END AS condrank
                                           FROM rep_agrre x
                                           WHERE x.sectisn=CondTab(indx).sectisn
                                          ) x 
                                    ) x 
                               WHERE x.condrank=x.condrank2agr
                              )
                    SELECT --+ ordered use_nl ( x bc ) index( bc X_REPBUH2COND_COND )
                           bc.*
                    FROM x,repbuh2cond bc
                    WHERE bc.agrisn=x.agrisn
                      AND x.condisn IS NOT NULL
                      AND bc.condisn=x.condisn
                      AND bc.statcode IN (38, 34, 221, 241)
                      AND bc.sagroup IN (1, 3)
                      AND bc.dateval<= vDateRep
                    UNION ALL
                    SELECT --+ ordered use_nl ( x bc ) index( bc X_REPBUH2COND_AGRISN )
                           bc.*
                    FROM x, repbuh2cond bc
                    WHERE bc.agrisn=x.agrisn
                      AND x.condisn IS NULL
                      AND bc.statcode IN (38, 34, 221, 241)
                      AND bc.sagroup IN (1, 3)
                      AND bc.dateval<= vDateRep
                   ) bc;
            ELSE
              SELECT --+ no_merge ( x )
                      SUM((SELECT --+ index( rnp X_RNP_RSBU_AGRISN )
                                  NVL(SUM(rnp.bruttosum*rnp.bruttopc*rnp.k_rnp_rsbucond),0)
                           FROM rnp_rsbu rnp
                           WHERE rnp.agrisn=x.agrisn
                             AND (x.condisn IS NULL OR rnp.condisn=x.condisn)
                             AND rnp.loadisn=CondTab(indx).LoadIsn
                             AND rnp.sagroup=1
                             AND rnp.isdar IS NULL -- EGAO 17.12.2013
                          )),
                      MAX((SELECT --+ index (bc X_REPBUH2COND_AGRISN)
                                  NVL(SUM(DISTINCT bc.isrevaluation),0)
                           FROM repbuh2cond bc
                           WHERE bc.agrisn=x.agrisn
                             AND bc.sagroup=1
                             AND (x.condisn IS NULL OR bc.condisn=x.condisn)
                          ))
              INTO SectRec.BruttoRNP, SectRec.Isrevaluation
              FROM (SELECT --+ index ( x X_AGRX_SECT )
                           DISTINCT
                           x.agrisn,
                           x.condisn
                    FROM rep_agrre x
                    WHERE x.sectisn=CondTab(indx).sectisn
                   ) x;
                   
              SELECT NVL(-SUM(CASE a.buhcurrisn
                                WHEN LocalCurr THEN a.amount
                                ELSE gcc2.gcc2(a.amount,a.buhcurrisn,LocalCurr, CASE SectRec.Isrevaluation
                                                                                  WHEN 0 THEN CondTab(indx).sectdatebeg
                                                                                  ELSE vDateRep
                                                                                END)
                              END*a.prorata),0)
              INTO SectRec.ReNachPrem
              FROM (
                    SELECT --+ index (a X_REPBUHRE2RESECTION_SECT) use_hash ( a h )
                            a.buhcurrisn, a.amount,
                            CASE
                              WHEN months_between(a.sectdateend, a.sectdatebeg)>13 THEN
                                CASE
                                  WHEN vDateRep>a.reaccdatebeg THEN greatest(0, (a.reaccdateend-vDateRep))
                                  ELSE greatest(1,(a.reaccdateend-a.reaccdatebeg+1))
                                END/greatest(1,(a.reaccdateend-a.reaccdatebeg+1))
                              ELSE
                                CASE
                                  WHEN vDateRep>a.sectdatebeg THEN greatest(0, (a.sectdateend-vDateRep))
                                  ELSE greatest(1,(a.sectdateend-a.sectdatebeg+1))
                                END/greatest(1,(a.sectdateend-a.sectdatebeg+1))
                            END AS prorata 
                    FROM repbuhre2resection_new a /*EGAO 18.12.2013 repbuhre2resection a*/, buhsubacc h
                    WHERE a.sectisn=CondTab(indx).sectisn
                      AND h.ISN=a.subaccisn
                      AND (h.ID LIKE '924%' OR h.id LIKE '914%' OR h.id LIKE '916%')
                      AND a.dateval<=vDateRep
                      AND a.dsclassisn=414
                      AND nvl(a.dsclassisn2,0)<>2265208403 -- EGAO 14.03.2013
                   ) a;
            END IF;
            SectRec.Duration :=greatest(1, (CondTab(indx).sectdateend-CondTab(indx).sectdatebeg+1));
            SectRec.Remainder:=CASE
                                 WHEN vDateRep>CondTab(indx).sectdatebeg THEN greatest(0, (CondTab(indx).sectdateend-vDateRep))
                                 ELSE SectRec.Duration
                               END ;
            SectRec.ProRata := SectRec.Remainder/SectRec.Duration;
            
          ELSIF CondTab(indx).reclassisn = AgrObligType
            AND NOT ((CondTab(indx).redatebase='I' AND
                      NOT (CondTab(indx).agrdatebeg BETWEEN CondTab(indx).sectdatebeg AND CondTab(indx).sectdateend)) OR
                     (CondTab(indx).redatebase='C' AND CondTab(indx).sectdateend<vDaterep)) THEN
            IF CondTab(indx).secttype IN ('XL', 'SL') THEN
              SELECT COUNT(DECODE(cd.rate,NULL,1)), MAX(cd.epi)
              INTO SectRec.EmptyRateLayerCnt, SectRec.EPI
              FROM recond cd
              WHERE cd.sectisn=CondTab(indx).sectisn
                AND cd.isn NOT IN (119901171303, 111824735603) -- EGAO 11.01.2012 Лейеры заведены некорректно, исправлению в АИС не подлежат. Исключаем их обработку в коде
              ;

              IF SectRec.EmptyRateLayerCnt>0 AND SectRec.EPI IS NULL THEN
                SELECT NVL(SUM(decode(bc.buhcurrisn,CondTab(indx).sectcurrisn, amount,gcc2.gcc2(bc.amount,bc.buhcurrisn,CondTab(indx).sectcurrisn,bc.dateval))),0)
                INTO SectRec.NachPrem
                FROM (
                      WITH x AS (SELECT DISTINCT
                                        x.agrisn,
                                        x.condisn
                                 FROM (SELECT x.*,
                                              MIN(x.condrank) over (PARTITION BY agrisn) AS condrank2agr
                                       FROM (SELECT --+ index ( x X_REP_AGRRE_SECTISN )
                                                    x.agrisn, x.condisn,
                                                    CASE WHEN x.condisn IS NULL THEN 1 ELSE 2 END AS condrank
                                             FROM rep_agrre x
                                             WHERE x.sectisn=CondTab(indx).sectisn
                                            ) x 
                                      ) x 
                                 WHERE x.condrank=x.condrank2agr
                                )
                      SELECT --+ ordered use_nl ( x bc ) index( bc X_REPBUH2COND_COND )
                             bc.*
                      FROM x,repbuh2cond bc
                      WHERE bc.agrisn=x.agrisn
                        AND x.condisn IS NOT NULL
                        AND bc.condisn=x.condisn
                        AND bc.statcode IN (38, 34, 221, 241)
                        AND bc.sagroup=1
                        AND bc.dateval<= vDateRep
                      UNION ALL
                      SELECT --+ ordered use_nl ( x bc ) index( bc X_REPBUH2COND_AGRISN )
                             bc.*
                      FROM x, repbuh2cond bc
                      WHERE bc.agrisn=x.agrisn
                        AND x.condisn IS NULL
                        AND bc.statcode IN (38, 34, 221, 241)
                        AND bc.sagroup=1
                        AND bc.dateval<= vDateRep
                     ) bc;
              END IF;
            END IF;
          END IF;
        ELSE
          SectRec.BruttoRNP := SectTab(to_char(CondTab(indx).sectisn)).BruttoRNP;
          SectRec.Isrevaluation := SectTab(to_char(CondTab(indx).sectisn)).Isrevaluation;
          SectRec.NachPrem := SectTab(to_char(CondTab(indx).sectisn)).NachPrem;
          SectRec.Duration := SectTab(to_char(CondTab(indx).sectisn)).Duration;
          SectRec.Remainder:= SectTab(to_char(CondTab(indx).sectisn)).Remainder;
          SectRec.ProRata := SectTab(to_char(CondTab(indx).sectisn)).ProRata;
          SectRec.EmptyRateLayerCnt := SectTab(to_char(CondTab(indx).sectisn)).EmptyRateLayerCnt;
          SectRec.EPI := SectTab(to_char(CondTab(indx).sectisn)).EPI;
          SectRec.ReNachPrem := SectTab(to_char(CondTab(indx).sectisn)).ReNachPrem;
        END IF;

        SELECT /*+ index ( a X_TT_RNP_RE_RSBU_PREVSECTAM )*/
               NVL(SUM(a.rernp),0)
        INTO SectRec.RNPPrevAmount
        FROM tt_rnp_re_rsbu_prevsectamount a
        WHERE a.agrisn=CondTab(indx).agrisn
          AND a.condisn=CondTab(indx).condisn
          AND a.sectpriority<CondTab(indx).sectpriority;

        --{EGAO 17.10.2011 IF (CondTab(indx).condbruttornp-SectRec.RNPPrevAmount)>0 THEN
        IF NOT (CondTab(indx).condbruttornp=0 OR SectRec.RNPPrevAmount/CondTab(indx).condbruttornp >1) THEN
        --}
          IF CondTab(indx).reclassisn = -1 THEN -- перестрахование по схеме "Участники"
            --{ EGAO 14.08.2013 в рамках  ДИТ-13-3-205032 
            /*SELECT --+ index ( rl X_AGRROLE_AGR )
                   NVL(SUM(decode(rl.sumclassisn2,414, rl.sharepc,8133016,rl.sharepc))/100 , 0) sharepc
            INTO SectRec.RNPShare
            FROM agrrole rl
            WHERE rl.agrisn=CondTab(indx).agrisn
              AND rl.orderno>0
              AND rl.classisn=435
              AND rl.sumclassisn=414
              AND rl.sharepc<>0
              AND rl.calcflg='Y';*/
            SectRec.RNPShare := 0;
            --} EGAO 14.08.2013  

            SectRec.RNPShare_t := SectRec.RNPShare;
          ELSIF CondTab(indx).reclassisn = AgrFacultType THEN
            IF CondTab(indx).agrruleisn=CosmicRuleAgr THEN
              SectRec.RNPShare := CASE WHEN SectRec.NachPrem<=0 THEN 0 ELSE least(1,SectRec.ReNachPrem/SectRec.NachPrem) END;  
            ELSE
              SectRec.RNPShare := CASE WHEN SectRec.BruttoRNP=0 THEN 0 ELSE SectRec.ReNachPrem/SectRec.BruttorNP END;
            END IF;  
            SectRec.RNPShare_t := SectRec.RNPShare;
          ELSIF CondTab(indx).reclassisn = AgrObligType THEN
            IF (CondTab(indx).redatebase='I' AND
                  --{ EGAO 05.10.2011
                  /*NOT (CondTab(indx).datebeg<=CondTab(indx).sectdateend AND
                       CondTab(indx).dateend>=CondTab(indx).sectdatebeg)*/
                  NOT (CondTab(indx).agrdatebeg BETWEEN CondTab(indx).sectdatebeg AND CondTab(indx).sectdateend)
                  --}
               ) OR
               (CondTab(indx).redatebase='C' AND CondTab(indx).sectdateend<vDaterep) THEN

                SectRec.RNPShare := 0;
                SectRec.RNPShare_t:=SectRec.RNPShare;

            ELSE
              vLayerShare := 0;
              IF CondTab(indx).secttype='RX' THEN
                OPEN cond2sect_cur(CondTab(indx).agrisn, CondTab(indx).condisn, CondTab(indx).secttype, CondTab(indx).agrdatebeg, CondTab(indx).conddatebeg);
                FETCH cond2sect_cur INTO cond2sect_rec;


                OPEN layer_cur(CondTab(indx).sectisn, CondTab(indx).redatebase, CondTab(indx).agrdatebeg, CondTab(indx).conddatebeg, CondTab(indx).sectdatebeg, CondTab(indx).sectdateend);
                LOOP
                  FETCH layer_cur INTO layer_rec;
                  EXIT WHEN layer_cur%NOTFOUND;

                  IF cond2sect_rec.SectType='QS' THEN
                      vLayerShare := CASE
                                      WHEN CondTab(indx).sisum=0 OR cond2sect_rec.sharepc>=1 THEN 0
                                      ELSE least(greatest((CondTab(indx).sisum*(1-cond2sect_rec.sharepc)-layer_rec.prioritysum)/(CondTab(indx).sisum*(1-cond2sect_rec.sharepc)),0
                                                         ),layer_rec.limitsum/(CondTab(indx).sisum*(1-cond2sect_rec.sharepc)))
                                    END;
                  ELSIF cond2sect_rec.SectType='XL' THEN
                    vPrioritySum := CASE
                                      WHEN cond2sect_rec.currisn=CondTab(indx).sectcurrisn THEN cond2sect_rec.prioritysum
                                      ELSE gcc2.gcc2(cond2sect_rec.prioritysum,cond2sect_rec.currisn,CondTab(indx).sectcurrisn,cond2sect_rec.datebeg)
                                    END;
                    vLayerShare := CASE
                                    WHEN vPrioritySum=0 THEN 0
                                    ELSE least(greatest((vPrioritySum-layer_rec.PrioritySum)/vPrioritySum,0
                                                       ), layer_rec.limitsum/vPrioritySum)
                                  END;
                  ELSE
                    vLayerShare := CASE
                                    WHEN CondTab(indx).sisum=0 THEN 0
                                    ELSE least(greatest((CondTab(indx).sisum-layer_rec.prioritysum)/CondTab(indx).sisum,0
                                                       ), layer_rec.limitsum/CondTab(indx).sisum)
                                  END;
                  END IF;
                  SectRec.RNPShare := SectRec.RNPShare + layer_rec.rate*vLayerShare*layer_rec.sharepc;
                  SectRec.RNPShare_t := SectRec.RNPShare_t + layer_rec.rate*vLayerShare;

                END LOOP;

                SectRec.RNPShare := SectRec.RNPShare*CondTab(indx).shareneorig;
                SectRec.RNPShare_t := SectRec.RNPShare_t*CondTab(indx).shareneorig;

              ELSIF CondTab(indx).secttype='QS' THEN
                OPEN cond2sect_cur(CondTab(indx).agrisn, CondTab(indx).condisn, CondTab(indx).secttype, CondTab(indx).agrdatebeg, CondTab(indx).conddatebeg);
                FETCH cond2sect_cur INTO cond2sect_rec;


                vPrioritySum := CASE
                                  WHEN CondTab(indx).sectcurrisn=cond2sect_rec.currisn THEN cond2sect_rec.prioritysum
                                  ELSE gcc2.GCC2(cond2sect_rec.prioritysum, cond2sect_rec.currisn, CondTab(indx).sectcurrisn, CondTab(indx).sicurrdate)
                                END;


                OPEN layer_cur(CondTab(indx).sectisn, CondTab(indx).redatebase, CondTab(indx).agrdatebeg, CondTab(indx).conddatebeg, CondTab(indx).sectdatebeg, CondTab(indx).sectdateend);
                LOOP
                  FETCH layer_cur INTO layer_rec;
                  EXIT WHEN layer_cur%NOTFOUND;

                  IF layer_rec.rateisnull =1 OR CondTab(indx).shareneorig<>1 THEN -- EGAO 18.02.2010
                    IF layer_rec.limitsumisnull = 1 THEN -- EGAO 23.06.2010 (дополнение в ТЗ, сделанное Дмитревской, после случая с договором УМС01/05, у которого не был указан лимит секции)
                      vLayerShare := 1;
                    ELSE
                      IF cond2sect_rec.SectType='RX' THEN
                        vLayerShare := CASE
                                         WHEN vPrioritySum=0 THEN 1
                                         ELSE least(1, layer_rec.limitsum/vPrioritySum)
                                       END;
                      ELSIF cond2sect_rec.SectType='QS' THEN
                        vLayerShare :=  CASE
                                          WHEN CondTab(indx).sisum=0  THEN 1
                                          WHEN cond2sect_rec.sharepc>=1 THEN 0 -- 29.04.2010
                                          ELSE least(1,(layer_rec.limitsum/(CondTab(indx).sisum*(1-cond2sect_rec.sharepc))))
                                        END;

                      ELSIF cond2sect_rec.SectType='XL' THEN
                        vLayerShare := CASE
                                         WHEN vPrioritySum=0 THEN 1
                                         ELSE least(1, layer_rec.limitsum/vPrioritySum)
                                       END;


                      ELSE
                        vLayerShare := CASE
                                         WHEN CondTab(indx).sisum=0 THEN 1
                                         ELSE least(1, layer_rec.limitsum/CondTab(indx).sisum)
                                       END;
                      END IF;
                    END IF;
                    SectRec.RNPShare := SectRec.RNPShare + vLayerShare*layer_rec.sharepc*CondTab(indx).shareneorig;
                  ELSE
                    SectRec.RNPShare := SectRec.RNPShare + (1- layer_rec.rate); -- EGAO 17.08.2011 (1- layer_rec.rate);
                  END IF;
                END LOOP;
                SectRec.RNPShare_t := SectRec.RNPShare;


              ELSIF CondTab(indx).secttype='SP' THEN
                OPEN cond2sect_cur(CondTab(indx).agrisn, CondTab(indx).condisn, CondTab(indx).secttype, CondTab(indx).agrdatebeg, CondTab(indx).conddatebeg);
                FETCH cond2sect_cur INTO cond2sect_rec;

                vPrioritySum := CASE
                                   WHEN CondTab(indx).sectcurrisn=cond2sect_rec.currisn THEN cond2sect_rec.prioritysum
                                   ELSE gcc2.GCC2(cond2sect_rec.prioritysum, cond2sect_rec.currisn, CondTab(indx).sectcurrisn, CondTab(indx).sicurrdate)
                                 END;


                OPEN layer_cur(CondTab(indx).sectisn, CondTab(indx).redatebase, CondTab(indx).agrdatebeg, CondTab(indx).conddatebeg, CondTab(indx).sectdatebeg, CondTab(indx).sectdateend);
                LOOP
                  FETCH layer_cur INTO layer_rec;
                  EXIT WHEN layer_cur%NOTFOUND;
                  IF layer_rec.limitsumisnull = 1 THEN -- EGAO 23.06.2010 (дополнение в ТЗ, сделанное Дмитревской, после случая с договором УМС01/05, у которого не был указан лимит секции)
                    vLayerShare := CASE
                                     WHEN CondTab(indx).sisum=0 THEN 1
                                     ELSE greatest((CondTab(indx).sisum-layer_rec.prioritysum)/CondTab(indx).sisum,0)
                                   END;
                  ELSE

                    IF cond2sect_rec.SectType='RX' THEN
                      vLayerShare := CASE
                                       WHEN vPrioritySum=0 THEN 0
                                       ELSE least(greatest((vPrioritySum-layer_rec.prioritysum)/vPrioritySum,0),(layer_rec.limitsum-layer_rec.prioritysum)/vPrioritySum)
                                     END;
                    ELSIF cond2sect_rec.SectType='QS' THEN
                      vLayerShare := CASE
                                       WHEN CondTab(indx).sisum=0 OR cond2sect_rec.sharepc>=1  THEN 0
                                       ELSE least(greatest((CondTab(indx).sisum*(1-cond2sect_rec.sharepc)-layer_rec.prioritysum)/(CondTab(indx).sisum*(1-cond2sect_rec.sharepc)),0
                                                          ),(layer_rec.limitsum-layer_rec.prioritysum)/(CondTab(indx).sisum*(1-cond2sect_rec.sharepc)))
                                     END;
                    ELSIF cond2sect_rec.SectType='XL' THEN
                      vLayerShare := CASE
                                       WHEN vPrioritySum=0 THEN 0
                                       ELSE least(greatest((vPrioritySum-layer_rec.prioritysum)/vPrioritySum,0),(layer_rec.limitsum-layer_rec.prioritysum)/vPrioritySum)
                                     END;

                    ELSE
                      vLayerShare := CASE
                                       WHEN CondTab(indx).sisum=0 THEN 1
                                       ELSE least(greatest((CondTab(indx).sisum-layer_rec.prioritysum)/CondTab(indx).sisum,0),(layer_rec.limitsum-layer_rec.prioritysum)/CondTab(indx).sisum)
                                     END;

                    END IF;
                  END IF;
                  SectRec.RNPShare := SectRec.RNPShare + vLayerShare*layer_rec.sharepc;
                END LOOP;


                SectRec.RNPShare := SectRec.RNPShare*CondTab(indx).shareneorig;
                SectRec.RNPShare_t := SectRec.RNPShare;
              ELSIF CondTab(indx).secttype IN ('XL', 'SL') THEN
                IF SectRec.EmptyRateLayerCnt>0 AND SectRec.EPI IS NOT NULL THEN
                  OPEN layer_cur(CondTab(indx).sectisn, CondTab(indx).redatebase, CondTab(indx).agrdatebeg, CondTab(indx).conddatebeg, CondTab(indx).sectdatebeg, CondTab(indx).sectdateend);
                  LOOP
                    FETCH layer_cur INTO layer_rec;
                    EXIT WHEN layer_cur%NOTFOUND;
                    vLayerShare := vLayerShare + layer_rec.depospremsum*layer_rec.sharepc;
                  END LOOP;

                  SectRec.RNPShare := CondTab(indx).shareneorig*vLayerShare/SectRec.EPI;

                ELSIF SectRec.EmptyRateLayerCnt>0 THEN
                  IF SectRec.NachPrem=0 THEN
                    SectRec.RNPShare := 0;
                  ELSE
                    OPEN layer_cur(CondTab(indx).sectisn, CondTab(indx).redatebase, CondTab(indx).agrdatebeg, CondTab(indx).conddatebeg, CondTab(indx).sectdatebeg, CondTab(indx).sectdateend);
                    LOOP
                      FETCH layer_cur INTO layer_rec;
                      EXIT WHEN layer_cur%NOTFOUND;
                      vLayerShare := vLayerShare + layer_rec.depospremsum*layer_rec.sharepc;
                    END LOOP;


                    SectRec.RNPShare := vLayerShare/SectRec.NachPrem*CondTab(indx).shareneorig;
                  END IF;
                ELSE
                  OPEN layer_cur(CondTab(indx).sectisn, CondTab(indx).redatebase, CondTab(indx).agrdatebeg, CondTab(indx).conddatebeg, CondTab(indx).sectdatebeg, CondTab(indx).sectdateend);
                  LOOP
                    FETCH layer_cur INTO layer_rec;
                    EXIT WHEN layer_cur%NOTFOUND;
                    vLayerShare := vLayerShare + layer_rec.rate*layer_rec.sharepc;
                  END LOOP;

                  SectRec.RNPShare := vLayerShare*CondTab(indx).shareneorig;
                END IF;
                SectRec.RNPShare_t := SectRec.RNPShare;
              END IF;
            END IF;
          END IF;
        END IF;


        SectTab(to_char(CondTab(indx).sectisn)) := SectRec;
        vCondIsn := CondTab(indx).condisn;
        vAgrIsn := CondTab(indx).agrisn;
        vSectIsn := CondTab(indx).sectisn;
      END IF;
      --{EGAO 17.10.2011
      /*IF (CondTab(indx).condbruttornp-SectTab(to_char(CondTab(indx).sectisn)).RNPPrevAmount)>0 THEN

        IF CondTab(indx).bruttornp < 0 THEN
          vReRNP:= CondTab(indx).bruttornp*SectTab(to_char(CondTab(indx).sectisn)).RNPShare*(1-CondTab(indx).sectcommission/100);
          vReRNP_t := CondTab(indx).bruttornp*SectTab(to_char(CondTab(indx).sectisn)).RNPShare_t*(1-CondTab(indx).sectcommission/100);
        ELSE
          vReRNP := (CondTab(indx).bruttornp-SectTab(to_char(CondTab(indx).sectisn)).RNPPrevAmount)*SectTab(to_char(CondTab(indx).sectisn)).RNPShare*(1-CondTab(indx).sectcommission/100);
          vReRNP_t := (CondTab(indx).bruttornp-SectTab(to_char(CondTab(indx).sectisn)).RNPPrevAmount)*SectTab(to_char(CondTab(indx).sectisn)).RNPShare_t*(1-CondTab(indx).sectcommission/100);
        END IF;
      END IF;*/
      
      -- EGAO 19.01.2012
      SELECT /*+ index ( a X_TT_RNP_RE_RSBU_PREVSECTAM )*/
             NVL(SUM(a.rernp),0)
      INTO ContRNPPrevAmount
      FROM tt_rnp_re_rsbu_prevsectamount a
      WHERE a.agrisn=CondTab(indx).agrisn
        AND a.condisn=CondTab(indx).condisn
        AND a.riskclassisn=CondTab(indx).riskclassisn
        AND a.objisn=CondTab(indx).objisn 
        AND a.deptisn=CondTab(indx).deptisn
        AND a.rptgroupisn=CondTab(indx).rptgroupisn
        AND a.sagroup=CondTab(indx).sagroup
        AND a.statcode=CondTab(indx).statcode
        AND a.rptclass=CondTab(indx).rptclass
        AND a.isrevaluation=CondTab(indx).isrevaluation
        AND a.buhcurrisn=CondTab(indx).buhcurrisn
        --{EGAO 24.06.2013
        AND a.ruleisn=CondTab(indx).ruleisn
        AND a.subaccisn=CondTab(indx).subaccisn
        AND a.parentobjclassisn=CondTab(indx).parentobjclassisn
        AND a.objclassisn=CondTab(indx).objclassisn
        AND a.rptclassisn=CondTab(indx).rptclassisn
        --}EGAO 24.06.2013
        AND a.sectpriority<CondTab(indx).sectpriority;
      
      IF CondTab(indx).condbruttornp=0 THEN
        vReRNP := 0;
        vReRNP_t := 0;
      ELSIF SectTab(to_char(CondTab(indx).sectisn)).RNPPrevAmount/CondTab(indx).condbruttornp >1 THEN
        vReRNP := 0;
        vReRNP_t := 0;
      ELSE
        /*IF CondTab(indx).bruttornp < 0 THEN
          vReRNP:= CondTab(indx).bruttornp*SectTab(to_char(CondTab(indx).sectisn)).RNPShare*(1-CondTab(indx).sectcommission/100);
          vReRNP_t := CondTab(indx).bruttornp*SectTab(to_char(CondTab(indx).sectisn)).RNPShare_t*(1-CondTab(indx).sectcommission/100);
        ELSE*/
          --{EGAO 19.01.2012
          /*vReRNP := (CondTab(indx).bruttornp-SectTab(to_char(CondTab(indx).sectisn)).RNPPrevAmount)*SectTab(to_char(CondTab(indx).sectisn)).RNPShare*(1-CondTab(indx).sectcommission/100);
          vReRNP_t := (CondTab(indx).bruttornp-SectTab(to_char(CondTab(indx).sectisn)).RNPPrevAmount)*SectTab(to_char(CondTab(indx).sectisn)).RNPShare_t*(1-CondTab(indx).sectcommission/100);*/
          vReRNP := (CondTab(indx).bruttornp-ContRNPPrevAmount)*SectTab(to_char(CondTab(indx).sectisn)).RNPShare*(1-CondTab(indx).sectcommission/100);
          vReRNP_t := (CondTab(indx).bruttornp-ContRNPPrevAmount)*SectTab(to_char(CondTab(indx).sectisn)).RNPShare_t*(1-CondTab(indx).sectcommission/100);
          --}
        /*END IF;*/
      END IF;
      
      
      IF CondTab(indx).secttype<>'RX' THEN
        MERGE INTO tt_rnp_re_rsbu_prevsectamount a
        USING (SELECT CondTab(indx).condisn AS condisn, 
                      CondTab(indx).agrisn AS agrisn,
                      CondTab(indx).riskclassisn AS riskclassisn,
                      CondTab(indx).objisn AS objisn,
                      CondTab(indx).deptisn AS deptisn,
                      CondTab(indx).rptgroupisn AS rptgroupisn,
                      CondTab(indx).sagroup AS sagroup,
                      CondTab(indx).statcode AS statcode,
                      CondTab(indx).rptclass AS rptclass, 
                      CondTab(indx).isrevaluation AS isrevaluation,
                      CondTab(indx).buhcurrisn AS buhcurrisn,
                      CondTab(indx).sectpriority AS sectpriority
                      --{EGAO 24.06.2013
                      ,CondTab(indx).ruleisn AS ruleisn
                      ,CondTab(indx).subaccisn AS subaccisn
                      ,CondTab(indx).parentobjclassisn AS parentobjclassisn
                      ,CondTab(indx).objclassisn AS objclassisn
                      ,CondTab(indx).rptclassisn AS rptclassisn
                      --}EGAO 24.06.2013
                      ,vReRNP_t AS rernp
               FROM dual
              ) b
        ON (a.condisn=b.condisn AND 
            a.agrisn=b.agrisn AND
            a.riskclassisn=a.riskclassisn AND
            a.objisn=b.objisn AND
            a.deptisn=b.deptisn AND
            a.rptgroupisn=b.rptgroupisn AND
            a.sagroup=b.sagroup AND
            a.statcode=b.statcode AND
            a.rptclass=b.rptclass AND
            a.isrevaluation=b.isrevaluation AND
            a.buhcurrisn=b.buhcurrisn AND
            a.sectpriority=b.sectpriority AND
            --{EGAO 24.06.2013
            a.ruleisn=b.ruleisn AND
            a.subaccisn=b.subaccisn AND 
            a.parentobjclassisn=b.parentobjclassisn AND
            a.objclassisn=b.objclassisn AND
            a.rptclassisn=b.rptclassisn
            --}EGAO 24.06.2013
           )
        WHEN MATCHED THEN UPDATE SET a.rernp=a.rernp+b.rernp    
        WHEN NOT MATCHED THEN INSERT (a.condisn, 
                                      a.agrisn,
                                      a.riskclassisn,
                                      a.objisn,
                                      a.deptisn,
                                      a.rptgroupisn,
                                      a.sagroup,
                                      a.statcode,
                                      a.rptclass,
                                      a.isrevaluation,
                                      a.buhcurrisn,
                                      a.sectpriority,
                                      a.rernp
                                      --{EGAO 24.06.2013
                                      ,a.ruleisn
                                      ,a.subaccisn
                                      ,a.parentobjclassisn
                                      ,a.objclassisn
                                      ,a.rptclassisn
                                      --}EGAO 24.06.2013
                                     )
        VALUES (b.condisn, 
                b.agrisn,
                b.riskclassisn,
                b.objisn,
                b.deptisn,
                b.rptgroupisn,
                b.sagroup,
                b.statcode,
                b.rptclass,
                b.isrevaluation,
                b.buhcurrisn,
                b.sectpriority,
                b.rernp
                --{EGAO 24.06.2013
                ,b.ruleisn
                ,b.subaccisn
                ,b.parentobjclassisn
                ,b.objclassisn
                ,b.rptclassisn
                --}EGAO 24.06.2013
                );
        --COMMIT;
      ELSE
        MERGE INTO tt_rnp_re_rsbu_prevsectamount a
        USING (SELECT CondTab(indx).condisn AS condisn, 
                      CondTab(indx).agrisn AS agrisn,
                      CondTab(indx).riskclassisn AS riskclassisn,
                      CondTab(indx).objisn AS objisn,
                      CondTab(indx).deptisn AS deptisn,
                      CondTab(indx).rptgroupisn AS rptgroupisn,
                      CondTab(indx).sagroup AS sagroup,
                      CondTab(indx).statcode AS statcode,
                      CondTab(indx).rptclass AS rptclass, 
                      CondTab(indx).isrevaluation AS isrevaluation,
                      CondTab(indx).buhcurrisn AS buhcurrisn,
                      CondTab(indx).sectpriority AS sectpriority,
                      vReRNP_t AS rernp,
                      SectTab(to_char(CondTab(indx).sectisn)).rnpshare AS rnpshare
                      --{EGAO 24.06.2013
                      ,CondTab(indx).ruleisn AS ruleisn
                      ,CondTab(indx).subaccisn AS subaccisn
                      ,CondTab(indx).parentobjclassisn AS parentobjclassisn
                      ,CondTab(indx).objclassisn AS objclassisn
                      ,CondTab(indx).rptclassisn AS rptclassisn
                      --}EGAO 24.06.2013
               FROM dual
              ) b
        ON (a.condisn=b.condisn AND 
            a.agrisn=b.agrisn AND
            a.riskclassisn=a.riskclassisn AND
            a.objisn=b.objisn AND
            a.deptisn=b.deptisn AND
            a.rptgroupisn=b.rptgroupisn AND
            a.sagroup=b.sagroup AND
            a.statcode=b.statcode AND
            a.rptclass=b.rptclass AND
            a.isrevaluation=b.isrevaluation AND
            a.buhcurrisn=b.buhcurrisn AND
            a.sectpriority=b.sectpriority AND
            --{EGAO 24.06.2013
            a.ruleisn=b.ruleisn AND
            a.subaccisn=b.subaccisn AND 
            a.parentobjclassisn=b.parentobjclassisn AND
            a.objclassisn=b.objclassisn AND
            a.rptclassisn=b.rptclassisn
            --}EGAO 24.06.2013
           )
        WHEN MATCHED THEN UPDATE SET a.rernp=CASE WHEN nvl(b.rnpshare,0)>nvl(a.rnpshare,0) THEN b.rernp ELSE a.rernp END,
                                     a.rnpshare=CASE WHEN nvl(b.rnpshare,0)>nvl(a.rnpshare,0) THEN b.rnpshare ELSE a.rnpshare END
        WHEN NOT MATCHED THEN INSERT (a.condisn, 
                                      a.agrisn,
                                      a.riskclassisn,
                                      a.objisn,
                                      a.deptisn,
                                      a.rptgroupisn,
                                      a.sagroup,
                                      a.statcode,
                                      a.rptclass,
                                      a.isrevaluation,
                                      a.buhcurrisn,
                                      a.sectpriority,
                                      a.rernp,
                                      a.rnpshare
                                      --{EGAO 24.06.2013
                                      ,a.ruleisn
                                      ,a.subaccisn
                                      ,a.parentobjclassisn
                                      ,a.objclassisn
                                      ,a.rptclassisn
                                      --}EGAO 24.06.2013
                                     )
        VALUES (b.condisn, 
                b.agrisn,
                b.riskclassisn,
                b.objisn,
                b.deptisn,
                b.rptgroupisn,
                b.sagroup,
                b.statcode,
                b.rptclass,
                b.isrevaluation,
                b.buhcurrisn,
                b.sectpriority,
                b.rernp,
                b.rnpshare
                --{EGAO 24.06.2013
                ,b.ruleisn
                ,b.subaccisn
                ,b.parentobjclassisn
                ,b.objclassisn
                ,b.rptclassisn
                --}EGAO 24.06.2013
                );
        --COMMIT;
      END IF;
      
      
      
      --}

      --}

      INSERT INTO rnp_re_rsbu(
        isn,
        loadisn,
        daterep,
        agrxisn,shareneorig,
        reisn,reid,redatebase,reclassisn,rernp,
        sectisn,sectfullname,secttype,sectdatebeg,sectdateend,sectcurrisn,sectobjisn,sectriskisn,
        sectcommission,sectbruttornp,sectisrevaluation,sectpriority,
        agrisn,agrid,agrdatebeg,agrdateend,
        objisn, riskisn, deptisn, rptgroupisn, bruttornp,
        reschema, sectrnpprevamount, sectduration, sectremainder, sectprorata,
        sharernp,sharernp_t,sisum, sicurrdate, sagroup, statcode, riskclassisn, buhcurrisn,
        condisn, sectnachprem, rptclass, isrevaluation, sectrenachprem, agrruleisn
        --{EGAO 24.06.2013
        ,ruleisn
        ,subaccisn
        ,parentobjclassisn
        ,objclassisn
        ,issub
        ,clientjuridical
        ,clientorgformisn
        ,rptclassisn
        --}EGAO 24.06.2013
        ,agrclassisn -- EGAO 19.07.2013
        ,istender -- EGAO 19.07.2013
        --{EGAO 15.08.2013
        ,conddatebeg
        ,conddateend
        --}EGAO 15.08.2013
        )
      VALUES(seq_rnp_re_rsbu.nextval,
             CondTab(indx).Loadisn,
             vDateRep,
             CondTab(indx).agrxisn,
             CondTab(indx).shareneorig,
             CondTab(indx).reisn,
             CondTab(indx).reid,
             CondTab(indx).redatebase,
             CondTab(indx).reclassisn,
             vReRNP,
             CondTab(indx).sectisn,
             CondTab(indx).sectfullname,
             CondTab(indx).secttype,
             CondTab(indx).sectdatebeg,
             CondTab(indx).sectdateend,
             CondTab(indx).sectcurrisn,
             CondTab(indx).sectobjisn,
             CondTab(indx).sectriskisn,
             CondTab(indx).sectcommission,
             SectTab(to_char(CondTab(indx).sectisn)).BruttoRNP,
             SectTab(to_char(CondTab(indx).sectisn)).IsRevaluation,
             CondTab(indx).sectpriority,
             CondTab(indx).agrisn,
             CondTab(indx).agrid,
             CondTab(indx).agrdatebeg,
             CondTab(indx).agrdateend,
             CondTab(indx).objisn,
             CondTab(indx).riskisn,
             CondTab(indx).deptisn,
             CondTab(indx).rptgroupisn,
             CondTab(indx).bruttornp,
             CondTab(indx).reschema,
             ContRNPPrevAmount,
             SectTab(to_char(CondTab(indx).sectisn)).Duration,
             SectTab(to_char(CondTab(indx).sectisn)).Remainder,
             SectTab(to_char(CondTab(indx).sectisn)).ProRata,
             SectTab(to_char(CondTab(indx).sectisn)).RNPShare,
             SectTab(to_char(CondTab(indx).sectisn)).RNPShare_t,
             CondTab(indx).sisum,
             CondTab(indx).sicurrdate,
             CondTab(indx).sagroup,
             CondTab(indx).statcode,
             CondTab(indx).riskclassisn,
             CondTab(indx).buhcurrisn,
             CondTab(indx).condisn,
             SectTab(to_char(CondTab(indx).sectisn)).NachPrem,
             CondTab(indx).rptclass,
             CondTab(indx).isrevaluation,
             SectTab(to_char(CondTab(indx).sectisn)).ReNachPrem, 
             CondTab(indx).agrruleisn
             --{EGAO 24.06.2013
             ,CondTab(indx).ruleisn
             ,CondTab(indx).subaccisn
             ,CondTab(indx).parentobjclassisn
             ,CondTab(indx).objclassisn
             ,CondTab(indx).issub
             ,CondTab(indx).clientjuridical
             ,CondTab(indx).clientorgformisn
             ,CondTab(indx).rptclassisn
             --}EGAO 24.06.2013
             ,CondTab(indx).agrclassisn -- EGAO 19.07.2013
             ,CondTab(indx).istender -- EGAO 19.07.2013
             ,CondTab(indx).conddatebeg
             ,CondTab(indx).conddateend
             );
      IF layer_cur%ISOPEN THEN CLOSE layer_cur; END IF;
      IF cond2sect_cur%ISOPEN THEN CLOSE cond2sect_cur; END IF;
    
    END LOOP;
    COMMIT;
  END;

  PROCEDURE make_rnp_re_msfo(pLoadIsn NUMBER := NULL)
  IS
    vLoadIsn NUMBER := nvl(pLoadIsn, GetActiveLoad(trunc(SYSDATE,'mm')-1));

    --vMinIsn NUMBER :=0;
    --vMaxIsn NUMBER;
    --vSql    VARCHAR2(4000);
    --SesId   NUMBER;
    --vLoadObjCnt NUMBER := 100000;
    vPart VARCHAR2(30);
  BEGIN
    IF vLoadIsn IS NULL  THEN
      raise_application_error(en_invalid_loadisn,'Invalid loadisn');
    END IF;


    replog_i (vLoadIsn, 'LoadRNPNEW', 'make_rnp_re_msfo_virtualcond',pAction=>'Begin');
    make_rnp_re_msfo_virtualcond(vLoadIsn);
    replog_i (vLoadIsn, 'LoadRNPNEW', 'make_rnp_re_msfo_virtualcond',pAction=>'End');

    replog_i (vLoadIsn, 'LoadRNPNEW', 'make_rnp_re_msfo_xl',pAction=>'Begin');
    make_rnp_re_msfo_xl(vLoadIsn);
    replog_i (vLoadIsn, 'LoadRNPNEW', 'make_rnp_re_msfo_xl',pAction=>'End');


    /*SesId:=Parallel_Tasks.createnewsession();

    vPart:=init_partition_by_key(pTableName => 'storages.rnp_re_msfo',pKey => vLoadIsn,pCompress => 1);
    dbms_lock.sleep(20);
    replog_i (vLoadIsn, 'LoadRNPNEW', 'make_rnp_re_msfo_by_isn',pAction=>'Begin');
    LOOP
      vMaxIsn:=Cut_Table('storages.repbuh2cond','agrisn',vMinIsn,pRowCount=>vLoadObjCnt);

      EXIT WHEN vMaxIsn IS NULL;

      vSql:= 'DECLARE
                vLoadIsn number :='||vLoadIsn||';
                vMinIsn number :='||vMinIsn||';
                vMaxIsn number :='||vMaxIsn||';
                vCnt    number :='||vCnt||';
              BEGIN
                DBMS_APPLICATION_INFO.SET_MODULE(''rnp_re_msfo_by_isn'',''Precess#''||vCNT);
                storages.report_rnp_new.make_rnp_re_msfo_by_isn(pLoadIsn => vLoadIsn,
                                        pminisn => vMinIsn,
                                        pmaxisn => vMaxIsn);
              END;';

      System.Parallel_Tasks.processtask(sesid,vsql);
      vCnt:=vCnt+1;
      vMinIsn:=vMaxIsn;

      DBMS_APPLICATION_INFO.set_module('rnp_re_msfo','Applied: '||vCnt*vLoadObjCnt);

    END LOOP;
    -- ждем, пока завершатся все джобы
    Parallel_Tasks.endsession(sesid);
    replog_i (vLoadIsn, 'LoadRNPNEW', 'make_rnp_re_msfo_by_isn',pAction=>'End');

    replog_i (vLoadIsn, 'LoadRNPNEW', 'make_rnp_re_msfo_final',pAction=>'Begin');
    make_rnp_re_msfo_final_old(vLoadIsn);
    replog_i (vLoadIsn, 'LoadRNPNEW', 'make_rnp_re_msfo_final',pAction=>'End');*/

    replog_i (vLoadIsn, 'LoadRNPNEW', 'make_rnp_re_msfo_prem',pAction=>'Begin');
    make_rnp_re_msfo_prem(vLoadIsn);
    replog_i (vLoadIsn, 'LoadRNPNEW', 'make_rnp_re_msfo_prem',pAction=>'End');

    replog_i (vLoadIsn, 'LoadRNPNEW', 'make_rnp_re_msfo_rnp',pAction=>'Begin');
    make_rnp_re_msfo_rnp(vLoadIsn);
    replog_i (vLoadIsn, 'LoadRNPNEW', 'make_rnp_re_msfo_rnp',pAction=>'End');

    replog_i (vLoadIsn, 'LoadRNPNEW', 'make_rnp_re_msfo_final',pAction=>'Begin');
    make_rnp_re_msfo_final(vLoadIsn);
    replog_i (vLoadIsn, 'LoadRNPNEW', 'make_rnp_re_msfo_final',pAction=>'End');


  END;
  
  PROCEDURE make_rnp_re_msfo_virtualcond(pLoadisn IN NUMBER)
  IS
    vDateRep DATE := GetDateRep(pLoadIsn);
    vMinIsn     NUMBER :=-1;
    vMaxIsn     NUMBER;
    vSql        VARCHAR2(4000);
    SesId       NUMBER;
    vLoadObjCnt NUMBER := 50000;
    vCnt        NUMBER:=0;
    vPart VARCHAR2(30);
  BEGIN
    IF vDateRep IS NULL THEN
      raise_application_error(en_invalid_daterep,'Invalid daterep');
    END IF;
    SesId:=Parallel_Tasks.createnewsession;

    replog_i (pLoadIsn, 'LoadRNPNEW', 'rnp_re_msfo_virtualcond',pAction=>'Begin');
    vPart:=init_partition_by_key(pTableName => 'storages.rnp_re_msfo_virtualcond',pKey => pLoadIsn,pCompress => 1);
    
    LOOP
      vMaxIsn:=Cut_Table('storages.rep_agrre','agrisn',vMinIsn,pRowCount=>vLoadObjCnt);

      EXIT WHEN vMaxIsn IS NULL;

      vSql:= 'DECLARE
                vMinIsn NUMBER :='||vMinIsn||';
                vMaxIsn NUMBER :='||vMaxIsn||';
                vCnt    NUMBER :='||vCnt||';
                vLoadIsn NUMBER :='||pLoadisn||';
                vDateRep DATE := to_date('''||to_char(vDateRep,'dd.mm.yyyy')||''',''dd.mm.yyyy'');
              BEGIN
                DBMS_APPLICATION_INFO.SET_MODULE(''rnp_re_msfo'',''VirtualCondMain. Process:''||vCNT);
                storages.report_rnp_new.make_rnp_re_msfo_vcond_by_agr(vLoadisn, vMinIsn, vMaxIsn);
              END;';

      System.Parallel_Tasks.processtask(sesid,vsql);
      vCnt:=vCnt+1;
      vMinIsn:=vMaxIsn;

      DBMS_APPLICATION_INFO.set_module('rnp_re_msfo','Applied: '||vCnt*vLoadObjCnt);

    END LOOP;
    -- ждем, пока завершатся все джобы
    Parallel_Tasks.endsession(sesid);
    replog_i (pLoadIsn, 'LoadRNPNEW', 'rnp_re_msfo_virtualcond',pAction=>'End');

  END;
  
  PROCEDURE make_rnp_re_msfo_vcond_by_agr(pLoadisn IN NUMBER, pMinIsn IN NUMBER, pMaxIsn IN NUMBER)
  IS
    vDateRep DATE := GetDateRep(pLoadIsn);
  BEGIN
    IF vDateRep IS NULL THEN
      raise_application_error(en_invalid_daterep,'Invalid daterep');
    END IF;

    pparam.clear;
    pparam.SetParamN('MinAgrIsn',pMinIsn);
    pparam.SetParamN('MaxAgrIsn',pMaxIsn);
                
    INSERT INTO rnp_re_msfo_virtualcond(isn,
                                        condisn,
                                        agrisn,
                                        datebeg,
                                        dateend,
                                        sharepc,
                                        daterep,
                                        virtualdatebeg,
                                        virtualdateend, loadisn)
    SELECT seq_recond_msfo.nextval, a.condisn, a.agrisn, a.datebeg, a.dateend, a.sharepc,
           vDateRep, a.virtualdatebeg, a.virtualdateend, pLoadIsn
    FROM v_rnp_re_msfo_virtualcond a;
    COMMIT;
  END;
  
  PROCEDURE make_rnp_re_msfo_xl(pLoadIsn IN NUMBER)
  IS
    vDateRep DATE := GetDateRep(pLoadIsn);

    vSql        VARCHAR2(4000);
    SesId       NUMBER;
    TYPE TSectTab IS TABLE OF NUMBER INDEX BY BINARY_INTEGER;
    SectTab TSectTab;
    vPart VARCHAR2(30);
  BEGIN
    IF vDateRep IS NULL THEN
      raise_application_error(en_invalid_daterep,'Invalid daterep');
    END IF;

    vPart:=init_partition_by_key(pTableName => 'storages.rnp_re_msfo_xl_section',pKey => pLoadIsn,pCompress => 1);

    SELECT --+ ordered use_nl ( s ra cd pr )
           cd.sectisn
    BULK COLLECT INTO SectTab
    FROM resection s,
         repagr ra,
         recond cd,
         resubjperiod pr
    WHERE s.secttype IN ('XL','SL')-- EGAO 03.07.2012 s.secttype='XL'
      AND ra.agrisn=s.agrisn
      AND ra.datebase='C'
      AND ra.classisn=9018
      AND cd.sectisn=s.isn
      AND pr.condisn=cd.isn
    GROUP BY cd.sectisn
    HAVING COUNT(DECODE(cd.rate,NULL,1))>0;

    IF SectTab.count=0 THEN
      RETURN;
    END IF;

    SesId:=Parallel_Tasks.createnewsession;
    FOR i IN SectTab.FIRST .. SectTab.LAST LOOP
      vSql:= 'DECLARE
                  vSectIsn number :='||SectTab(i)||';
                  vCnt    number :='||i||';
                  vLoadIsn number :='||pLoadisn||';
                BEGIN
                  DBMS_APPLICATION_INFO.SET_MODULE(''rnp_re_msfo'',''xl_section Process:''||vCNT);
                  storages.report_rnp_new.make_rnp_re_msfo_xl_by_sect(vLoadisn, vSectIsn);
                END;';
      System.Parallel_Tasks.processtask(sesid,vsql);
      DBMS_APPLICATION_INFO.set_module('rnp_re_msfo','xl_section. Applied: '||i);
    END LOOP;
    -- ждем, пока завершатся все джобы
    Parallel_Tasks.endsession(sesid);

  END;


  PROCEDURE make_rnp_re_msfo_xl_by_sect(pLoadisn IN NUMBER, pSectIsn IN NUMBER)
  IS
    vDateRep DATE := GetDateRep(pLoadIsn);

    CURSOR agr_cur
    IS
    SELECT a.*, COUNT(1) over () AS cnt, rownum AS rn
    FROM (
           SELECT DISTINCT agrisn
           FROM rep_agrre a
           WHERE a.sectisn=pSectIsn
         ) a;
    TYPE TAgrTab IS TABLE OF agr_cur%ROWTYPE INDEX BY BINARY_INTEGER;
    AgrTab TAgrTab;

    CURSOR cond_cur(agrisn_in IN NUMBER)
    IS
    SELECT --+ index ( bc X_REPBUH2COND_COND ) ordered use_nl ( a bc ) no_merge( a )
           a.agrisn,
           a.condisn,
           a.sectisn,
           MAX(a.sdatebeg) AS sectdatebeg,
           MAX(a.sdateend) AS sectdateend,
           a.virtualdatebeg,
           a.virtualdateend,
           CASE WHEN months_between(a.virtualdateend+1,a.virtualdatebeg)>13 THEN 'L' ELSE 'G' END AS lengthtype,
           NVL(SUM(bc.amount*a.sharepc),0) AS buhamount,
           NVL(MAX(bc.condpremagr*a.sharepc),0) AS premiumsum, --EGAO 06.12.2011 MAX(bc.condpremagr) AS premiumsum,
           NVL(SUM(CASE
                     WHEN bc.buhcurrisn=bc.agrcurrisn THEN bc.amount
                     ELSE gcc2.gcc2(bc.buhamount,bc.buhcurrisn,bc.agrcurrisn, bc.dateval)
                   END*a.sharepc),0) AS buhamountagr,
           NVL(SUM(CASE
                     WHEN bc.buhcurrisn=53 THEN bc.amount
                     ELSE gcc2.gcc2(bc.amount,bc.buhcurrisn,53, bc.dateval)
                   END*a.sharepc),0) AS buhamountusd
    FROM (
          WITH x AS (SELECT --+  index ( x X_REP_AGRRE_SECTISN )
                             x.*
                     FROM rep_agrre x
                     WHERE x.sectisn=pSectIsn
                       AND x.agrisn=agrisn_in
                    ),
               a AS (SELECT --+ index (a X_RNP_RE_MSFO_VCOND_AGR)
                            a.*
                     FROM rnp_re_msfo_virtualcond a
                     WHERE a.loadisn=pLoadIsn
                       AND a.agrisn=agrisn_in
                    )         ,
               rc AS (SELECT --+ index ( rc X_REPCOND_AGR )
                             rc.*
                      FROM repcond rc
                      WHERE rc.agrisn=agrisn_in
                     )
          SELECT --+ use_hash ( x a )
                 x.agrisn, a.condisn,x.sectisn, x.sdatebeg, x.sdateend, a.virtualdatebeg, a.virtualdateend, a.sharepc
          FROM x,
               a
          WHERE a.agrisn=x.agrisn
            AND (x.condisn IS NULL OR x.condisn=a.condisn)
          UNION ALL
          SELECT --+ ordered use_hash ( x a rc )
                 x.agrisn, rc.condisn,x.sectisn, x.sdatebeg, x.sdateend, rc.datebeg, rc.dateend, 1 AS sharepc
          FROM x,
               a,
               rc
          WHERE a.agrisn(+)=x.agrisn
            AND x.condisn IS NULL
            AND a.isn IS NULL
            AND rc.agrisn=x.agrisn
          UNION ALL
          SELECT --+ ordered use_hash ( x a rc )
                 x.agrisn, rc.condisn,x.sectisn, x.sdatebeg, x.sdateend, rc.datebeg, rc.dateend, 1 AS sharepc
          FROM x,
               a,
               rc
          WHERE a.agrisn(+)=x.agrisn
            AND a.condisn(+)=x.condisn
            AND x.condisn IS NOT NULL
            AND a.isn IS NULL
            AND rc.agrisn=x.agrisn
            AND rc.condisn=x.condisn
         ) a, repbuh2cond bc
    WHERE bc.agrisn=a.agrisn
      AND bc.condisn=a.condisn
      AND bc.dateval<=vDateRep
      AND bc.sagroup IN (1, 3)
      AND bc.statcode IN (38, 34, 221, 241)
    GROUP BY a.agrisn, a.condisn, a.sectisn, a.virtualdatebeg, a.virtualdateend;


    TYPE TCondTab IS TABLE OF cond_cur%ROWTYPE INDEX BY BINARY_INTEGER;
    CondTab TCondTab;

    TYPE TCoverageRec IS RECORD(
      Premium NUMBER,
      Rnp NUMBER,
      CondDateBeg DATE,
      CondDateEnd DATE,
      CondDuration NUMBER,
      CondRemainder NUMBER
    );
    CoverageRec TCoverageRec;

    vSectDuration NUMBER;
    vSectRemainder NUMBER;

    TYPE TResultTab IS TABLE OF rnp_re_msfo_xl_section%ROWTYPE INDEX BY BINARY_INTEGER;
    ResultTab TResultTab;

    j BINARY_INTEGER;
    vIsn NUMBER;


  BEGIN
    IF vDateRep IS NULL THEN
      raise_application_error(en_invalid_daterep,'Invalid daterep');
    END IF;

    OPEN agr_cur;
    FETCH agr_cur BULK COLLECT INTO AgrTab;

    IF AgrTab.count=0 THEN
      IF agr_cur%ISOPEN THEN CLOSE agr_cur; END IF;
      RETURN;
    END IF;

    FOR n IN AgrTab.first..AgrTab.last LOOP
      ResultTab.delete;
      dbms_application_info.set_module('rnp_re_msfo '||pSectIsn||'-'||AgrTab(n).agrisn,'xl_section: '||to_char(AgrTab(n).rn) ||' of '||to_char(AgrTab(n).cnt));
      OPEN cond_cur(AgrTab(n).agrisn);
      FETCH cond_cur BULK COLLECT INTO CondTab;
      IF CondTab.count>0 THEN
        FOR i IN CondTab.FIRST .. CondTab.LAST LOOP
          IF vSectRemainder IS NULL THEN
            vSectDuration :=greatest(1, (CondTab(i).SectDateEnd-CondTab(i).SectDateBeg+1));
            vSectRemainder:=CASE
                              WHEN vDateRep>CondTab(i).SectDateBeg THEN greatest(0, (CondTab(i).SectDateEnd-vDateRep))
                              ELSE vSectDuration
                            END;
          END IF;
          CoverageRec.CondDateBeg := CondTab(i).virtualdatebeg;
          IF CondTab(i).lengthtype = 'L' AND
             CondTab(i).premiumsum<>0 AND
             CondTab(i).buhamountagr/CondTab(i).premiumsum > 0 AND
             CondTab(i).buhamountagr/CondTab(i).premiumsum < 1 THEN
            CoverageRec.CondDuration := (CondTab(i).virtualdateend-CondTab(i).virtualdatebeg+1)*least(1, CondTab(i).buhamountagr/CondTab(i).premiumsum);
            CoverageRec.CondDateEnd := CoverageRec.CondDateBeg+CoverageRec.CondDuration-1;
          ELSE
            CoverageRec.CondDateEnd := CondTab(i).virtualdateend;
          END IF;

          CoverageRec.CondDuration :=greatest(1, (CoverageRec.CondDateEnd-CoverageRec.CondDateBeg+1));
          CoverageRec.CondRemainder:=CASE
                                       WHEN vDateRep>CoverageRec.CondDateBeg THEN greatest(0, (CoverageRec.CondDateEnd-vDateRep))
                                       ELSE CoverageRec.CondDuration
                                     END;

          CoverageRec.Premium := greatest(0, (least(CondTab(i).SectDateEnd,CoverageRec.CondDateEnd)-greatest(CondTab(i).SectDateBeg,CoverageRec.CondDateBeg)+1)/CoverageRec.CondDuration);
          CoverageRec.Rnp := CASE
                               WHEN CoverageRec.CondRemainder=0 THEN 0
                               ELSE least(vSectRemainder, CoverageRec.CondRemainder)/CoverageRec.CondRemainder
                             END;

          j := ResultTab.count+1;
          SELECT seq_recond_msfo.nextval INTO vIsn FROM dual;
          ResultTab(j).isn:=vIsn;
          ResultTab(j).loadisn:=ploadisn;
          ResultTab(j).agrisn := CondTab(i).agrisn;
          ResultTab(j).condisn := CondTab(i).condisn;
          ResultTab(j).virtualdatebeg := CondTab(i).virtualdatebeg;
          ResultTab(j).virtualdateend := CondTab(i).virtualdateend;
          ResultTab(j).sectisn := CondTab(i).sectisn;
          ResultTab(j).coverageprem := CoverageRec.Premium;
          ResultTab(j).coveragernp := CoverageRec.Rnp;
          ResultTab(j).buhamountusd := CondTab(i).buhamountusd;
        END LOOP;
      END IF;
      IF cond_cur%ISOPEN THEN
        CLOSE cond_cur;
      END IF;
      FORALL indx IN ResultTab.FIRST .. ResultTab.LAST
      INSERT INTO rnp_re_msfo_xl_section
      VALUES ResultTab(indx);
    END LOOP;

    COMMIT;
    IF agr_cur%ISOPEN THEN
      CLOSE agr_cur;
    END IF;

  END;


  PROCEDURE make_rnp_re_msfo_prem(pLoadIsn IN NUMBER)
  IS
    vDateRep DATE := GetDateRep(pLoadIsn);

    vMinIsn number:=0;
    vMaxIsn number;
    vSql varchar2(4000);
    SesId Number;
    vLoadObjCnt number:=100000;
    vCnt number:=0;
    vPart VARCHAR2(30);
  BEGIN
    IF vDateRep IS NULL THEN
      raise_application_error(en_invalid_daterep,'Invalid daterep');
    END IF;
    SesId:=Parallel_Tasks.createnewsession();

    vPart:=init_partition_by_key(pTableName => 'storages.rnp_re_msfo_premium',pKey => pLoadIsn,pCompress => 1);
    --dbms_lock.sleep(20);
    replog_i (pLoadIsn, 'LoadRNPNEW', 'make_rnp_re_msfo_prem',pAction=>'Begin');

    LOOP
      vMaxIsn:=Cut_Table('storages.repbuh2cond','agrisn',vMinIsn,pRowCount=>vLoadObjCnt);

      EXIT WHEN vMaxIsn IS NULL;

      vSql:= 'DECLARE
                vLoadIsn number :='||pLoadIsn||';
                vMinIsn number :='||vMinIsn||';
                vMaxIsn number :='||vMaxIsn||';
                vCnt    number :='||vCnt||';
              BEGIN
                DBMS_APPLICATION_INFO.SET_MODULE(''rnp_re_msfo_prem'',''Precess#''||vCNT);
                storages.report_rnp_new.make_rnp_re_msfo_prem_by_isn(pLoadIsn => vLoadIsn,
                                        pminisn => vMinIsn,
                                        pmaxisn => vMaxIsn);
              END;';

      System.Parallel_Tasks.processtask(sesid,vsql);
      vCnt:=vCnt+1;
      vMinIsn:=vMaxIsn;

      DBMS_APPLICATION_INFO.set_module('rnp_re_msfo','Applied: '||vCnt*vLoadObjCnt);

    END LOOP;
    -- ждем, пока завершатся все джобы
    Parallel_Tasks.endsession(sesid);
    replog_i (pLoadIsn, 'LoadRNPNEW', 'make_rnp_re_msfo_prem',pAction=>'End');
  END;

  PROCEDURE make_rnp_re_msfo_prem_by_isn(pLoadIsn IN NUMBER, pMinIsn IN NUMBER, pMaxIsn IN NUMBER)
  IS
    vDateRep DATE := GetDateRep(pLoadIsn);
    vXLShare NUMBER;
    vCondIsn       NUMBER := -1;
    vAgrIsn        NUMBER := -1;
    vVirtualDateBeg DATE := to_date('01.01.1900','dd.mm.yyyy');
    vVirtualDateEnd DATE := to_date('01.01.1900','dd.mm.yyyy');
    vIndex         BINARY_INTEGER;
    vPrioritySum   NUMBER := 0;
    vLayerShare    NUMBER := 0;
    vSectIsn NUMBER := -999;

    i NUMBER := gcc2.gcc2(1,LocalCurr,DollarCurr,SYSDATE);
    j NUMBER := 0;
    vIsn NUMBER := 0;

    vRePrem   NUMBER;
    vRePrem_t NUMBER;

    TYPE TCond IS TABLE OF v_rnp_re_msfo_premium%ROWTYPE;

    CondTab TCond;

    TYPE TPrevSectElement IS RECORD(
      Sharepc NUMBER,
      PremValue NUMBER
    );

    PrevSectElement TPrevSectElement;

    TYPE TPrevSectTab IS TABLE OF TPrevSectElement INDEX BY BINARY_INTEGER;

    PrevSectTab TPrevSectTab;

    TYPE TSectRec IS RECORD(
      Isn NUMBER,
      Duration NUMBER,
      Remainder NUMBER,
      ProRata NUMBER,
      BruttoRNP NUMBER,
      PremShare NUMBER, -- доля_в_усл_донач в формуле ВКЛАД_в_исх_премию
      PremShare_t NUMBER, -- доля_в_усл_донач в формуле исх_fac
      PremPrevAmount NUMBER, -- исх_fac
      NachPrem NUMBER, -- нач_премия_по_секции_без_восстановительной_до_отч_даты
      --LessThanAYearAgcCnt NUMBER,
      DepospremSum NUMBER, -- Сумма депозитной(флэт) премии секции
      EmptyRateLayerCnt NUMBER, -- кол-во лейеров секции с пустой ставкой перерасчета 
      vEPI NUMBER, -- EPI секции
      EmptyDeposPremLayerCnt NUMBER, -- кол-во лейеров секции с пустой депозитной(флэт) премией
      ReinsuranceAgrBuhAmount NUMBER, -- брутто-премия всех условий прямых/входящих договоров, перестрахованных указанной секцией
      ReinsuranceAgrEarnedAmount NUMBER, -- заработанная премия по всем перестрахованным условиям за время действия секции
      SharePc NUMBER -- сумма долей участников (размещение секции)
    );

    SectRec TSectRec;

    TYPE TSectTab IS TABLE OF TSectRec INDEX BY VARCHAR2(100);

    SectTab TSectTab;


    TYPE TCoverageRec IS RECORD(
      CondDateBeg DATE,
      CondDateEnd DATE,
      CondDuration NUMBER,
      CondRemainder NUMBER,
      Premium NUMBER, -- покрытие по времени доначисление
      Rnp NUMBER -- покрытие по времени РНП
    );

    CoverageRec TCoverageRec;

    CURSOR cond2sect_cur(agrisn_in NUMBER, condisn_in NUMBER, secttype_in IN VARCHAR2)
    IS
    SELECT MAX(s.secttype) AS secttype,
           MAX(s.datebeg) AS datebeg,
           MAX(s.currisn) AS currisn,
           NVL(MIN(cd.prioritysum),0) AS prioritysum,
           NVL(SUM(CASE WHEN pr.parentisn IS NULL THEN pr.sharepc END)/100,0) AS sharepc
    FROM (SELECT s.*,
                 rank() over (ORDER BY CASE secttype_in
                                         WHEN 'RX' THEN CASE s.secttype WHEN 'QS' THEN 1 WHEN 'XL' THEN 2 END
                                         WHEN 'QS' THEN CASE s.secttype WHEN 'RX' THEN 1 WHEN 'QS' THEN 2 WHEN 'XL' THEN 3  END
                                         WHEN 'SP' THEN CASE s.secttype WHEN 'RX' THEN 1 WHEN 'QS' THEN 2 WHEN 'XL' THEN 3  END
                                       END) AS rn
          FROM (SELECT --+ ordered use_nl ( x reagr agr ) index ( x X_REP_AGRRE_SECTISN )
                       x.sectisn,
                       x.secttype,
                       x.sectcurrisn AS currisn,
                       x.sdatebeg AS datebeg,
                       COUNT(CASE WHEN x.secttype='QS' THEN 1 END) over  (PARTITION BY x.secttype) AS qscnt,
                       COUNT(CASE WHEN x.secttype='XL' THEN 1 END) over  (PARTITION BY x.secttype) AS xlcnt,
                       COUNT(CASE WHEN x.secttype='RX' THEN 1 END) over  (PARTITION BY x.secttype) AS rxcnt
                 FROM rep_agrre x,
                      agreement reagr,
                      agreement agr
                 WHERE x.agrisn=agrisn_in
                   AND (x.condisn IS NULL OR x.condisn=condisn_in)
                   AND reagr.isn=x.reisn
                   --EGAO 24.08.2011 AND ((x.secttype IN ('QS','XL') AND reagr.classisn=9018) OR (x.secttype='RX'))
                   AND agr.isn=x.agrisn
                   AND CASE
                         WHEN reagr.datebase='C' AND vDateRep BETWEEN x.sdatebeg AND x.sdateend  THEN 1
                         WHEN reagr.datebase='I' AND agr.datebeg BETWEEN x.sdatebeg AND x.sdateend THEN 1
                         ELSE 0
                       END = 1
                   --{EGAO 24.08.2011
                   AND CASE secttype_in
                         WHEN 'RX' THEN CASE WHEN reagr.classisn=9018 AND x.secttype IN ('QS','XL') THEN 1 ELSE 0 END
                         WHEN 'QS' THEN CASE WHEN reagr.classisn=9058 AND x.secttype='RX' OR reagr.classisn=9018 AND x.secttype IN ('QS', 'XL') THEN 1 ELSE 0 END
                         WHEN 'SP' THEN CASE WHEN reagr.classisn=9058 AND x.secttype='RX' OR reagr.classisn=9018 AND x.secttype IN ('QS', 'XL') THEN 1 ELSE 0 END
                         ELSE 0
                       END = 1
                   --}

                ) s
          WHERE  qscnt>0 OR xlcnt=1 OR rxcnt>0 -- EGAO 17.05.2011 WHERE  (s.secttype='QS' AND qscnt>0) OR (s.secttype='XL' AND xlcnt=1) OR (rxcnt>0)
         ) s,
         recond cd,
         resubjperiod pr
    WHERE cd.sectisn=s.sectisn AND pr.condisn=cd.isn AND rn=1;

    cond2sect_rec cond2sect_cur%ROWTYPE;

    CURSOR layer_cur(sectisn_in NUMBER)
    IS
    --{EGAO 14.08.2013 т.к. закомментированный код не соответствует ТЗ
    /*SELECT cd.isn,
           MAX(decode(cd.rate,NULL,1,0)) AS rateisnull,
           MAX(decode(cd.limitsum,NULL,1,0)) AS limitsumisnull,
           NVL(MAX(cd.rate)/100,0) AS rate,
           NVL(MAX(cd.limitsum),0) AS limitsum,
           NVL(MAX(cd.prioritysum),0) AS prioritysum,
           NVL(max(cd.depospremsum),0) AS depospremsum,
           NVL(SUM(CASE WHEN pr.parentisn IS NULL THEN pr.sharepc END)/100,0) AS sharepc
    FROM recond cd,
         resubjperiod pr
    WHERE cd.sectisn=sectisn_in
      AND pr.condisn=cd.isn
    GROUP BY cd.isn;*/
    
    SELECT a.condisn, 
           MAX(decode(a.rate,NULL,1,0)) AS rateisnull,
           MAX(decode(a.limitsum,NULL,1,0)) AS limitsumisnull,
           NVL(MAX(a.rate)/100,0) AS rate,
           NVL(MAX(a.limitsum),0) AS limitsum,
           NVL(MAX(a.prioritysum),0) AS prioritysum,
           NVL(max(a.depospremsum),0) AS depospremsum,
           SUM(CASE WHEN nvl(a.condsharepc,0)=0 THEN 0 ELSE a.sharepc*a.parentsharepc/a.condsharepc END)/100 AS sharepc
    FROM (
           SELECT --+ ordered use_nl ( a cd )
                  a.condisn,
                  a.parentsharepc,
                  a.sharepc,
                  SUM(a.sharepc) over (PARTITION BY a.condisn, a.parentisn) AS condsharepc,
                  cd.rate, -- Премия лейера (в recond в %, здесь - доля)
                  cd.prioritysum, -- Приоритет лейера
                  cd.limitsum, -- Лимит лейера
                  cd.depospremsum-- Депозитная премия лейера
           FROM (
                 SELECT a.sectisn, -- секция
                        a.condisn, -- лейер
                        nvl(a.sharepc,0) AS sharepc, -- доля размещения участника
                        connect_by_root(nvl(a.sharepc,0)) AS parentsharepc, -- доля размещения брокера
                        connect_by_root(a.isn) AS parentisn
                 FROM resubjperiod a
                 WHERE CONNECT_BY_ISLEAF=1 AND nvl(a.sharepc,0)<>0
                 CONNECT BY PRIOR a.isn=a.parentisn
                 START WITH a.parentisn IS NULL AND nvl(a.sharepc,0)<>0 AND a.sectisn=sectisn_in 
                ) a,
                recond cd
           WHERE cd.isn=a.condisn
             AND cd.isn NOT IN (119901171303, 111824735603) -- EGAO 11.01.2012 Лейеры заведены некорректно, исправлению в АИС не подлежат. Исключаем их обработку в коде
         ) a
    GROUP BY a.condisn;
    
    
    

    --}EGAO 14.08.2013

    layer_rec layer_cur%ROWTYPE;

    TYPE TResultTab IS TABLE OF rnp_re_msfo_premium%ROWTYPE INDEX BY BINARY_INTEGER;

    ResultTab TResultTab;
  BEGIN
    IF vDateRep IS NULL THEN
      raise_application_error(en_invalid_daterep,'Invalid daterep');
    END IF;


    pparam.Clear;
    pparam.SetParamN('MinAgrIsn', pMinIsn);
    pparam.SetParamN('MaxAgrIsn', pMaxIsn);
    pparam.SetParamN('LoadIsn', pLoadIsn);
    pparam.SetParamD('DateRep', vDateRep);

    DELETE FROM tt_rnp_re_msfo_cond_premium;

    INSERT INTO tt_rnp_re_msfo_cond_premium (
      condisn, agrisn, datebeg, dateend, virtualdatebeg, virtualdateend, buhdeptisn,
      dateval, buhcurrisn, premiumsum, buhamount, agrcurrisn, agrclassisn,
      agrdatebeg, agrdateend, sharepc, rptgroupisn
    )
    SELECT condisn, agrisn, datebeg, dateend, virtualdatebeg, virtualdateend, buhdeptisn,
           dateval, buhcurrisn, premiumsum, buhamount, agrcurrisn, agrclassisn,
           agrdatebeg, agrdateend, sharepc, rptgroupisn
    FROM v_rnp_re_msfo_cond_premium;

    SELECT a.*
    BULK COLLECT INTO CondTab
    FROM v_rnp_re_msfo_premium a
    ORDER BY a.agrisn, a.condisn, a.virtualdatebeg, a.virtualdateend, a.sectpriority, a.sectisn;

    IF CondTab.count=0 THEN
      RETURN;
    END IF;


    FOR indx IN CondTab.first..CondTab.last  LOOP
      DBMS_APPLICATION_INFO.SET_MODULE('rnp_re_msfo_prem '||CondTab(indx).condisn,CondTab(indx).agrisn||'-'||CondTab(indx).sectisn);
      vLayerShare := 0;
      vRePrem := 0;
      vRePrem_t := 0;
      vPrioritySum := 0;

      IF CondTab(indx).agrisn<>vAgrIsn OR
         CondTab(indx).condisn<>vCondIsn OR
         CondTab(indx).virtualdatebeg<>vVirtualDateBeg OR
         CondTab(indx).virtualdateend<>vVirtualDateEnd OR
         CondTab(indx).sectisn<>vSectIsn THEN

        IF CondTab(indx).agrisn<>vAgrIsn OR
           CondTab(indx).condisn<>vCondIsn OR
           CondTab(indx).virtualdatebeg<>vVirtualDateBeg OR
           CondTab(indx).virtualdateend<>vVirtualDateEnd
           THEN
          PrevSectTab.DELETE;
          --dbms_output.put_line(CondTab(indx).agrisn||' - '||CondTab(indx).condisn);
        END IF;

        SectRec := NULL;
        SectRec.PremShare := 0;
        SectRec.PremShare_t := 0;
        SectRec.PremPrevAmount := 0;

        IF CondTab(indx).agrisn<>vAgrIsn AND CondTab(indx).reclassisn=-1
           AND SectTab.exists(to_char(CondTab(indx).sectisn)) THEN
          SectTab.delete(to_char(CondTab(indx).sectisn));
        END IF;

        IF NOT SectTab.exists(to_char(CondTab(indx).sectisn)) THEN
          IF CondTab(indx).reclassisn = -1 THEN
            SELECT --+ index ( a X_REPBUHBODY_AGR ) no_merge ( h ) use_hash ( a h )
                   NVL(-SUM(CASE a.currisn
                              WHEN DollarCurr THEN a.amount
                              ELSE gcc2.gcc2(a.amount,a.currisn,DollarCurr, vDateRep)
                            END),0)
            INTO SectRec.NachPrem
            FROM repbuhbody a,
                 (SELECT h.isn FROM buhsubacc h WHERE h.id LIKE '924%' OR h.id LIKE '914%' OR h.id LIKE '916%') h
            WHERE a.agrisn=CondTab(indx).agrisn
              AND h.ISN=a.subaccisn
              AND a.dateval<=vDateRep
              AND a.dsclassisn=414
              AND a.dsclassisn2<>2265208403;

          ELSE
            SELECT --+ index ( a X_REPBUHRE2RESECTION_SECT ) no_merge ( h ) use_hash ( a h )
                   NVL(-SUM(CASE a.buhcurrisn
                              WHEN DollarCurr THEN a.amount
                              ELSE gcc2.gcc2(a.amount,a.buhcurrisn,DollarCurr, vDateRep)
                            END),0)
            INTO SectRec.NachPrem
            FROM repbuhre2resection_new a /*EGAO 18.12.2013 repbuhre2resection a*/,
                 (SELECT h.isn FROM buhsubacc h WHERE h.id LIKE '924%' OR h.id LIKE '914%' OR h.id LIKE '916%') h
            WHERE a.sectisn=CondTab(indx).sectisn
              AND h.ISN=a.subaccisn
              AND a.dateval<=vDateRep
              AND a.dsclassisn=414;

            SectRec.Duration :=greatest(1, (CondTab(indx).sectdateend-CondTab(indx).sectdatebeg+1));
            SectRec.Remainder:=CASE
                                 WHEN vDateRep>CondTab(indx).sectdatebeg THEN greatest(0, (CondTab(indx).sectdateend-vDateRep))
                                 ELSE SectRec.Duration
                               END;
            SELECT SUM(CASE CondTab(indx).sectcurrisn
                       WHEN DollarCurr THEN depospremsum
                       ELSE gcc2.gcc2(depospremsum,CondTab(indx).sectcurrisn,DollarCurr,vDateRep)
                     END*sharepc),
                     COUNT(DECODE(rate,NULL,1)),
                     MAX(epi),
                     COUNT(DECODE(depospremsum,NULL,1)),
                     NVL(MAX(sharepc),0) AS sharepc
            INTO SectRec.DepospremSum, SectRec.EmptyRateLayerCnt, SectRec.vEPI, SectRec.EmptyDeposPremLayerCnt, SectRec.SharePc
            FROM (
                  SELECT cd.isn,
                         MAX(cd.depospremsum) AS depospremsum,
                         NVL(SUM(CASE WHEN pr.parentisn IS NULL THEN pr.sharepc END)/100,0) AS sharepc,
                         MAX(cd.rate/100) AS rate,
                         MAX(cd.epi) AS epi
                  FROM recond cd,
                       resubjperiod pr
                  WHERE cd.sectisn=CondTab(indx).sectisn
                    AND pr.condisn=cd.isn
                  GROUP BY cd.isn
                 );

            /*IF CondTab(indx).redatebase='I' THEN
              SELECT COUNT(1)
              INTO SectRec.LessThanAYearAgcCnt
              FROM dual
              WHERE EXISTS (SELECT --+ index ( x X_AGRX_SECT ) ordered use_nl ( x ra )
                                   'x'
                            FROM agrx x, repagr ra
                            WHERE x.sectisn=CondTab(indx).sectisn
                            AND ra.agrisn=x.agrisn
                            AND months_between(vDateRep, trunc(ra.dateend))<12
                           );

            END IF;*/


            IF CondTab(indx).secttype IN ('XL','SL') AND SectRec.EmptyRateLayerCnt>0 AND NVL(SectRec.vEPI, 0)=0 AND CondTab(indx).redatebase='I' THEN
              --{ EGAO 22.05.2012 оптимизация запроса (isn секции на которой старый вариант затыкался - 156176144603
              /*SELECT --+ ordered use_nl ( x bc ) index( bc X_REPBUH2COND_AGRISN ) no_merge ( x )
                     NVL(SUM(decode(bc.buhcurrisn,CondTab(indx).sectcurrisn, bc.amount,gcc2.gcc2(bc.amount,bc.buhcurrisn,CondTab(indx).sectcurrisn,bc.dateval))*x.shareneorig),0)
              INTO SectRec.ReinsuranceAgrBuhAmount
              FROM
                   (SELECT --+ index ( x X_REP_AGRRE_SECTISN )
                           DISTINCT
                           x.agrisn,
                           x.condisn,
                           CASE x.reclassisn
                             WHEN 9018 THEN 1
                             WHEN 9058 THEN nvl(x.shareneorig/100, 1)
                           END AS shareneorig
                    FROM rep_agrre x
                    WHERE x.sectisn=CondTab(indx).sectisn
                   ) x,
                   repbuh2cond bc
              WHERE bc.agrisn=x.agrisn
                AND (x.condisn IS NULL OR bc.condisn=x.condisn)
                AND bc.statcode IN (38, 34, 221, 241)
                AND bc.sagroup IN (1, 3)
                AND bc.dateval<= vDateRep;*/
              
              
              WITH x AS (SELECT --+ index ( x X_REP_AGRRE_SECTISN )
                                DISTINCT x.agrisn, x.condisn,
                                         CASE x.reclassisn
                                           WHEN 9018 THEN 1
                                           WHEN 9058 THEN nvl(x.shareneorig/100, 1)
                                         END AS shareneorig
                         FROM rep_agrre x
                         WHERE x.sectisn=CondTab(indx).sectisn
                        )
              SELECT NVL(SUM(decode(bc.buhcurrisn,CondTab(indx).sectcurrisn, bc.amount,gcc2.gcc2(bc.amount,bc.buhcurrisn,CondTab(indx).sectcurrisn,bc.dateval))*bc.shareneorig),0)
              INTO SectRec.ReinsuranceAgrBuhAmount
              FROM (SELECT --+ index ( bc X_REPBUH2COND_COND ) ordered use_nl ( x bc )
                           bc.*, x.shareneorig
                    FROM x, repbuh2cond bc
                    WHERE bc.agrisn=x.agrisn
                      AND x.condisn IS NOT NULL 
                      AND bc.condisn=x.condisn
                      AND bc.statcode IN (38, 34, 221, 241)
                      AND bc.sagroup IN (1, 3)
                      AND bc.dateval<= vDateRep
                      UNION ALL
                    SELECT --+ index ( bc X_REPBUH2COND_AGRISN ) ordered use_nl ( x bc )
                           bc.*, x.shareneorig
                    FROM x, repbuh2cond bc
                    WHERE bc.agrisn=x.agrisn
                      AND x.condisn IS NULL
                      AND bc.statcode IN (38, 34, 221, 241)
                      AND bc.sagroup IN (1, 3)
                      AND bc.dateval<= vDateRep  
                    ) bc;  
              --}
            END IF;
            IF CondTab(indx).secttype IN ('XL','SL') AND SectRec.EmptyRateLayerCnt>0 AND CondTab(indx).redatebase='C' AND CondTab(indx).reclassisn=AgrFacultType THEN
              SELECT --+ index (a X_RNP_RE_MSFO_XL_SECTION_SECT )
                     NVL(SUM(a.buhamountusd*a.coverageprem),0)
              INTO SectRec.ReinsuranceAgrEarnedAmount
              FROM rnp_re_msfo_xl_section a
              WHERE a.loadisn=pLoadIsn
               AND a.sectisn=CondTab(indx).sectisn;

            END IF;
          END IF;
        ELSE
          SectRec.NachPrem := SectTab(to_char(CondTab(indx).sectisn)).NachPrem;
          SectRec.Duration := SectTab(to_char(CondTab(indx).sectisn)).Duration;
          SectRec.Remainder:= SectTab(to_char(CondTab(indx).sectisn)).Remainder;
          SectRec.DepospremSum := SectTab(to_char(CondTab(indx).sectisn)).DepospremSum;
          SectRec.EmptyRateLayerCnt := SectTab(to_char(CondTab(indx).sectisn)).EmptyRateLayerCnt;
          SectRec.vEPI := SectTab(to_char(CondTab(indx).sectisn)).vEPI;
          SectRec.EmptyDeposPremLayerCnt := SectTab(to_char(CondTab(indx).sectisn)).EmptyDeposPremLayerCnt;
          --SectRec.LessThanAYearAgcCnt := SectTab(to_char(CondTab(indx).sectisn)).LessThanAYearAgcCnt;
          --{EGAO 17.10.2011
          SectRec.ReinsuranceAgrBuhAmount := SectTab(to_char(CondTab(indx).sectisn)).ReinsuranceAgrBuhAmount;
          SectRec.ReinsuranceAgrEarnedAmount := SectTab(to_char(CondTab(indx).sectisn)).ReinsuranceAgrEarnedAmount;
          --}
          SectRec.SharePc := SectTab(to_char(CondTab(indx).sectisn)).SharePc;

        END IF;

        CoverageRec := NULL;

        vIndex := PrevSectTab.first;
        WHILE CondTab(indx).sectpriority > vIndex LOOP
          SectRec.PremPrevAmount := SectRec.PremPrevAmount + PrevSectTab(vIndex).premvalue;
          vIndex := PrevSectTab.next(vIndex);
        END LOOP;

        IF CondTab(indx).reclassisn = -1 THEN -- Перестрахование по схеме "Участники"
          SELECT --+ index ( rl X_AGRROLE_AGR )
                 NVL(SUM(decode(rl.sumclassisn2,414, rl.sharepc,8133016,rl.sharepc))/100 , 0) sharepc
          INTO SectRec.PremShare
          FROM agrrole rl
          WHERE rl.agrisn=CondTab(indx).agrisn
            AND rl.orderno>0
            AND rl.classisn=435
            AND rl.sumclassisn=414
            AND rl.sharepc<>0
            AND rl.calcflg='Y';
          SectRec.PremShare_t:=SectRec.PremShare;
        ELSE
          IF CondTab(indx).redatebase='I' THEN
            IF CondTab(indx).virtualdatebeg BETWEEN CondTab(indx).sectdatebeg AND CondTab(indx).sectdateend 
              OR CondTab(indx).agrdatebeg BETWEEN CondTab(indx).sectdatebeg AND CondTab(indx).sectdateend -- EGAO 15.07.2013 в рамках 52198541303 
              THEN
              CoverageRec.Premium := 1;
              CoverageRec.Rnp := 1;
            ELSE
              CoverageRec.Premium := 0;
              CoverageRec.Rnp := 0;
            END IF;

          ELSIF CondTab(indx).redatebase='C' THEN
            CoverageRec.CondDateBeg := CondTab(indx).virtualdatebeg;
            IF CondTab(indx).lengthtype = 'L' AND
               CondTab(indx).premiumsum<>0 AND
               CondTab(indx).condbuhamountagr/CondTab(indx).premiumsum > 0 AND
               CondTab(indx).condbuhamountagr/CondTab(indx).premiumsum < 1 THEN -- по поводу CondTab(indx).premiumsum<>0 надо уточнить у заказчика
               CoverageRec.CondDuration := (CondTab(indx).virtualdateend-CondTab(indx).virtualdatebeg+1)*least(1, CondTab(indx).condbuhamountagr/CondTab(indx).premiumsum);
               CoverageRec.CondDateEnd := CoverageRec.CondDateBeg+CoverageRec.CondDuration-1;
            ELSE
              CoverageRec.CondDateEnd := CondTab(indx).virtualdateend;
            END IF;
            CoverageRec.CondDuration :=greatest(1, (CoverageRec.CondDateEnd-CoverageRec.CondDateBeg+1));
            CoverageRec.CondRemainder:=CASE
                                         WHEN vDateRep>CoverageRec.CondDateBeg THEN greatest(0, (CoverageRec.CondDateEnd-vDateRep))
                                         ELSE CoverageRec.CondDuration
                                       END;
            CoverageRec.Premium := greatest(0, (least(CondTab(indx).sectdateend,CoverageRec.CondDateEnd)-greatest(CondTab(indx).sectdatebeg,CoverageRec.CondDateBeg)+1)/CoverageRec.CondDuration);
            --{EGAO 03.07.2012
            CoverageRec.Rnp := CASE
                                 WHEN CoverageRec.CondRemainder=0 THEN 0
                                 ELSE least(SectRec.Remainder, CoverageRec.CondRemainder)/CoverageRec.CondRemainder
                               END;
            --}                   
          END IF;

          IF CondTab(indx).secttype='QS' THEN
            OPEN cond2sect_cur(CondTab(indx).agrisn, CondTab(indx).condisn,  CondTab(indx).sectisn);
            FETCH cond2sect_cur INTO cond2sect_rec;
            vPrioritySum := CASE
                              WHEN CondTab(indx).sectcurrisn=cond2sect_rec.currisn THEN cond2sect_rec.prioritysum
                              ELSE gcc2.GCC2(cond2sect_rec.prioritysum, cond2sect_rec.currisn, CondTab(indx).sectcurrisn, CondTab(indx).sicurrdate)
                            END;

            OPEN layer_cur(CondTab(indx).sectisn);
            FETCH layer_cur INTO layer_rec;
            IF layer_cur%FOUND THEN

              IF layer_rec.rateisnull =1 OR CondTab(indx).shareneorig<>1 THEN
                IF layer_rec.limitsumisnull = 1 OR CondTab(indx).reclassisn = AgrFacultType THEN
                  vLayerShare := 1;
                ELSE
                  IF cond2sect_rec.SectType='RX' THEN
                    vLayerShare := CASE
                                     WHEN vPrioritySum=0 THEN 1
                                     ELSE least(1, layer_rec.limitsum/vPrioritySum)
                                   END;
                  ELSIF cond2sect_rec.SectType='QS' THEN
                    vLayerShare :=  CASE
                                      WHEN CondTab(indx).sisum=0  THEN 1
                                      WHEN cond2sect_rec.sharepc>=1 THEN 0
                                      ELSE least(1,(layer_rec.limitsum/(CondTab(indx).sisum*(1-cond2sect_rec.sharepc))))
                                    END;
                  ELSIF cond2sect_rec.SectType='XL' THEN
                    vLayerShare := CASE
                                     WHEN vPrioritySum=0 THEN 1
                                     ELSE least(1, layer_rec.limitsum/vPrioritySum)
                                   END;
                  ELSE
                    vLayerShare := CASE
                                     WHEN CondTab(indx).sisum=0 THEN 1
                                     ELSE least(1, layer_rec.limitsum/CondTab(indx).sisum)
                                   END;
                  END IF;
                END IF;
                SectRec.PremShare := vLayerShare*layer_rec.sharepc*CondTab(indx).shareneorig*CoverageRec.Premium;
              ELSE
                --{ EGAO 03.07.2012
                --SectRec.PremShare := (1- layer_rec.rate)*CoverageRec.Premium;
                SectRec.PremShare := (1- layer_rec.rate);
                --}
              END IF;
              SectRec.PremShare_t := SectRec.PremShare;
            END IF;

          ELSIF CondTab(indx).secttype='SP' THEN
            OPEN cond2sect_cur(CondTab(indx).agrisn, CondTab(indx).condisn, CondTab(indx).secttype);
            FETCH cond2sect_cur INTO cond2sect_rec;
            vPrioritySum := CASE
                              WHEN CondTab(indx).sectcurrisn=cond2sect_rec.currisn THEN cond2sect_rec.prioritysum
                              ELSE gcc2.GCC2(cond2sect_rec.prioritysum, cond2sect_rec.currisn, CondTab(indx).sectcurrisn, CondTab(indx).sicurrdate)
                            END;
            OPEN layer_cur(CondTab(indx).sectisn);
            FETCH layer_cur INTO layer_rec;
            IF layer_cur%FOUND THEN
              IF layer_rec.limitsumisnull = 1 THEN -- EGAO 23.06.2010 (дополнение в ТЗ, сделанное Дмитревской, после случая с договором УМС01/05, у которого не был указан лимит секции)
                vLayerShare := CASE
                                 WHEN CondTab(indx).sisum=0 THEN 1
                                 ELSE greatest((CondTab(indx).sisum-layer_rec.prioritysum)/CondTab(indx).sisum,0)
                               END;
              ELSE
                IF CondTab(indx).reclassisn = AgrObligType AND cond2sect_rec.SectType='RX' THEN
                  vLayerShare := CASE
                                   WHEN vPrioritySum=0 THEN 0
                                   ELSE least(greatest((vPrioritySum-layer_rec.prioritysum)/vPrioritySum,0),(layer_rec.limitsum-layer_rec.prioritysum)/vPrioritySum)
                                 END;
                ELSIF CondTab(indx).reclassisn = AgrObligType AND cond2sect_rec.SectType='QS' THEN
                  vLayerShare := CASE
                                   WHEN CondTab(indx).sisum=0 OR cond2sect_rec.sharepc>=1  THEN 0
                                   ELSE least(greatest((CondTab(indx).sisum*(1-cond2sect_rec.sharepc)-layer_rec.prioritysum)/(CondTab(indx).sisum*(1-cond2sect_rec.sharepc)),0
                                                      ),(layer_rec.limitsum-layer_rec.prioritysum)/(CondTab(indx).sisum*(1-cond2sect_rec.sharepc)))
                                 END;
                ELSIF CondTab(indx).reclassisn = AgrObligType AND cond2sect_rec.SectType='XL' THEN
                  vLayerShare := CASE
                                   WHEN vPrioritySum=0 THEN 0
                                   ELSE least(greatest((vPrioritySum-layer_rec.prioritysum)/vPrioritySum,0),(layer_rec.limitsum-layer_rec.prioritysum)/vPrioritySum)
                                 END;
                ELSE
                  vLayerShare := CASE
                                   WHEN CondTab(indx).sisum=0 THEN 1
                                   ELSE least(greatest((CondTab(indx).sisum-layer_rec.prioritysum)/CondTab(indx).sisum,0),(layer_rec.limitsum-layer_rec.prioritysum)/CondTab(indx).sisum)
                                 END;
                END IF;
              END IF;
              SectRec.PremShare := vLayerShare*layer_rec.sharepc*CondTab(indx).shareneorig*CoverageRec.Premium;
              SectRec.PremShare_t := SectRec.PremShare;
            END IF;
          ELSIF CondTab(indx).secttype IN ('XL', 'SL') THEN
            IF SectRec.EmptyRateLayerCnt>0 AND NVL(SectRec.vEPI,0)<>0 AND
               (CondTab(indx).redatebase='I'  OR (CondTab(indx).redatebase='C' AND CondTab(indx).reclassisn=AgrObligType)) THEN
              OPEN layer_cur(CondTab(indx).sectisn);
              LOOP
                FETCH layer_cur INTO layer_rec;
                EXIT WHEN layer_cur%NOTFOUND;
                vLayerShare := vLayerShare + layer_rec.depospremsum*layer_rec.sharepc;
              END LOOP;
              SectRec.PremShare := CondTab(indx).shareneorig*vLayerShare/SectRec.vEPI*CoverageRec.Premium;
            ELSIF SectRec.EmptyRateLayerCnt>0 AND NVL(SectRec.vEPI, 0)=0 AND CondTab(indx).redatebase='I'  THEN
              IF SectRec.ReinsuranceAgrBuhAmount=0 THEN
                SectRec.PremShare := 0;
              ELSE
                OPEN layer_cur(CondTab(indx).sectisn);
                LOOP
                  FETCH layer_cur INTO layer_rec;
                  EXIT WHEN layer_cur%NOTFOUND;
                  vLayerShare := vLayerShare + layer_rec.depospremsum*layer_rec.sharepc;
                END LOOP;
                SectRec.PremShare := vLayerShare/SectRec.ReinsuranceAgrBuhAmount*CondTab(indx).shareneorig*CoverageRec.Premium;
              END IF;
            --{ EGAO 17.10.2011
            ELSIF SectRec.EmptyRateLayerCnt>0 AND CondTab(indx).redatebase='C' AND CondTab(indx).reclassisn=AgrFacultType THEN
              IF SectRec.ReinsuranceAgrEarnedAmount=0 THEN
                vXLShare := 0;
                vXLShare := 0;
              ELSE
                SELECT --+ index (a X_RNP_RE_MSFO_XL_SECTION_ASC )
                       NVL(SUM(a.buhamountusd*a.coverageprem)/SectRec.ReinsuranceAgrEarnedAmount,0)
                INTO vXLShare
                FROM rnp_re_msfo_xl_section a
                WHERE a.loadisn=pLoadIsn
                 AND a.agrisn=CondTab(indx).agrisn
                 AND a.sectisn=CondTab(indx).sectisn
                 AND a.condisn=CondTab(indx).condisn
                 --{EGAO 15.07.2013 в рамках 50384368403
                 AND a.virtualdatebeg=CondTab(indx).virtualdatebeg
                 AND a.virtualdateend=CondTab(indx).virtualdateend
                 --}EGAO 15.07.2013
                 ;
              END IF;
             --{EGAO 15.07.2013
             /* SectRec.PremShare := CASE
                                    WHEN SectRec.Duration=0 THEN 0
                                    ELSE SectRec.Depospremsum
                                  END*vXLShare;*/
             SectRec.PremShare := SectRec.Depospremsum*vXLShare;
             --}EGAO 15.07.2013                     
            --}
            ELSIF SectRec.EmptyRateLayerCnt=0 THEN
              OPEN layer_cur(CondTab(indx).sectisn);
              LOOP
                FETCH layer_cur INTO layer_rec;
                EXIT WHEN layer_cur%NOTFOUND;
                vLayerShare := vLayerShare + layer_rec.rate*layer_rec.sharepc;
              END LOOP;
              SectRec.PremShare := vLayerShare*CondTab(indx).shareneorig*CoverageRec.Premium;
            END IF;
            SectRec.PremShare_t := SectRec.PremShare;
          ELSIF CondTab(indx).secttype='RX' THEN
            IF (CondTab(indx).reclassisn=AgrObligType AND CondTab(indx).redatebase='I') OR
               (CondTab(indx).reclassisn=AgrObligType AND CondTab(indx).redatebase='C' AND SectRec.EmptyDeposPremLayerCnt=0 AND SectRec.EmptyRateLayerCnt=0) THEN
              OPEN cond2sect_cur(CondTab(indx).agrisn, CondTab(indx).condisn, CondTab(indx).secttype);
              FETCH cond2sect_cur INTO cond2sect_rec;
              OPEN layer_cur(CondTab(indx).sectisn);
              LOOP
                FETCH layer_cur INTO layer_rec;
                EXIT WHEN layer_cur%NOTFOUND;
                IF cond2sect_rec.SectType='QS' THEN
                    vLayerShare := CASE
                                    WHEN CondTab(indx).sisum=0 OR cond2sect_rec.sharepc>=1 THEN 0
                                    ELSE least(greatest((CondTab(indx).sisum*(1-cond2sect_rec.sharepc)-layer_rec.prioritysum)/(CondTab(indx).sisum*(1-cond2sect_rec.sharepc)),0
                                                       ),layer_rec.limitsum/(CondTab(indx).sisum*(1-cond2sect_rec.sharepc)))
                                  END;
                ELSIF cond2sect_rec.SectType='XL' THEN
                  vPrioritySum := CASE
                                    WHEN cond2sect_rec.currisn=CondTab(indx).sectcurrisn THEN cond2sect_rec.prioritysum
                                    ELSE gcc2.gcc2(cond2sect_rec.prioritysum,cond2sect_rec.currisn,CondTab(indx).sectcurrisn,cond2sect_rec.datebeg)
                                  END;
                  vLayerShare := CASE
                                  WHEN vPrioritySum=0 THEN 0
                                  ELSE least(greatest((vPrioritySum-layer_rec.PrioritySum)/vPrioritySum,0
                                                     ), layer_rec.limitsum/vPrioritySum)
                                END;
                ELSE
                  vLayerShare := CASE
                                  WHEN CondTab(indx).sisum=0 THEN 0
                                  ELSE least(greatest((CondTab(indx).sisum-layer_rec.prioritysum)/CondTab(indx).sisum,0
                                                     ), layer_rec.limitsum/CondTab(indx).sisum)
                                END;
                END IF;
                SectRec.PremShare := SectRec.PremShare + layer_rec.rate*vLayerShare*layer_rec.sharepc;
                SectRec.PremShare_t := SectRec.PremShare_t + layer_rec.rate*vLayerShare;
              END LOOP;
              SectRec.PremShare := SectRec.PremShare*CondTab(indx).shareneorig*CoverageRec.Premium;
              SectRec.PremShare_t := SectRec.PremShare_t*CondTab(indx).shareneorig*CoverageRec.Premium;
            END IF;
          END IF;
        END IF;

        PrevSectElement.SharePc := SectRec.SharePc;
        IF CondTab(indx).secttype IN ('XL','SL') AND SectRec.EmptyRateLayerCnt>0 AND CondTab(indx).redatebase='C' AND CondTab(indx).reclassisn=AgrFacultType THEN
          PrevSectElement.PremValue :=SectRec.PremShare;
        ELSE
          PrevSectElement.PremValue := (CondTab(indx).condbuhamount-SectRec.PremPrevAmount)*SectRec.PremShare_t*CondTab(indx).gn;
        END IF;
        IF PrevSectTab.exists(CondTab(indx).sectpriority) THEN
          IF CondTab(indx).secttype<>'RX' THEN
            PrevSectTab(CondTab(indx).sectpriority).premvalue := PrevSectTab(CondTab(indx).sectpriority).premvalue + PrevSectElement.PremValue;
          ELSIF CondTab(indx).secttype='RX' AND PrevSectTab(CondTab(indx).sectpriority).SharePc<PrevSectElement.SharePc THEN
            PrevSectTab(CondTab(indx).sectpriority) := PrevSectElement;
          END IF;
        ELSE
          PrevSectTab(CondTab(indx).sectpriority) := PrevSectElement;
        END IF;

        SectTab(to_char(CondTab(indx).sectisn)):= SectRec;
        vAgrIsn := CondTab(indx).agrisn;
        vCondIsn := CondTab(indx).condisn;
        vVirtualDateBeg := CondTab(indx).virtualdatebeg;
        vVirtualDateEnd := CondTab(indx).virtualdateend;
        vSectIsn := CondTab(indx).sectisn;
      END IF;

      /*IF (CondTab(indx).redatebase='I' AND SectTab(to_char(CondTab(indx).sectisn)).LessThanAYearAgcCnt=1) OR
         (CondTab(indx).redatebase='C' AND months_between(vDateRep, CondTab(indx).sectdateend)<24) THEN*/
        IF CondTab(indx).secttype IN ('XL','SL') AND SectTab(to_char(CondTab(indx).sectisn)).EmptyRateLayerCnt>0 AND CondTab(indx).redatebase='C' AND CondTab(indx).reclassisn=AgrFacultType THEN
          vRePrem := SectTab(to_char(CondTab(indx).sectisn)).PremShare;
          vRePrem_t := SectTab(to_char(CondTab(indx).sectisn)).PremShare_t;
        ELSE
          IF CondTab(indx).buhamount<0 THEN
            vRePrem := CondTab(indx).buhamount*SectTab(to_char(CondTab(indx).sectisn)).PremShare*CondTab(indx).gn;
            vRePrem_t := CondTab(indx).buhamount*SectTab(to_char(CondTab(indx).sectisn)).PremShare_t*CondTab(indx).gn;
          ELSE
            vRePrem := (CondTab(indx).buhamount-SectTab(to_char(CondTab(indx).sectisn)).PremPrevAmount)*SectTab(to_char(CondTab(indx).sectisn)).PremShare*CondTab(indx).gn;
            vRePrem_t := (CondTab(indx).buhamount-SectTab(to_char(CondTab(indx).sectisn)).PremPrevAmount)*SectTab(to_char(CondTab(indx).sectisn)).PremShare_t*CondTab(indx).gn;
            

          END IF;

        END IF;
      /*END IF;*/

      j := ResultTab.count+1;
      SELECT seq_rnp_re_msfo.nextval INTO vIsn FROM dual;
      ResultTab(j).isn:=vIsn;
      ResultTab(j).loadisn := pLoadIsn;
      ResultTab(j).daterep := vDateRep;
      -- соответствие прямог и исх. договоров
      ResultTab(j).agrxisn := CondTab(indx).agrxisn;
      ResultTab(j).shareneorig := CondTab(indx).shareneorig;
      -- исх. договор
      ResultTab(j).reisn := CondTab(indx).reisn;
      ResultTab(j).reid := CondTab(indx).reid;
      ResultTab(j).redatebase := CondTab(indx).redatebase;
      ResultTab(j).reclassisn := CondTab(indx).reclassisn;
      ResultTab(j).redatebeg := CondTab(indx).redatebeg;
      ResultTab(j).redateend := CondTab(indx).redateend;
      -- секция
      ResultTab(j).sectisn := CondTab(indx).sectisn;
      ResultTab(j).sectfullname := CondTab(indx).sectfullname;
      ResultTab(j).secttype := CondTab(indx).secttype;
      ResultTab(j).sectdatebeg := CondTab(indx).sectdatebeg;
      ResultTab(j).sectdateend := CondTab(indx).sectdateend;
      ResultTab(j).sectcurrisn := CondTab(indx).sectcurrisn;
      ResultTab(j).sectobjisn := CondTab(indx).sectobjisn;
      ResultTab(j).sectriskisn := CondTab(indx).sectriskisn;
      ResultTab(j).sectcommission := CondTab(indx).sectcommission;
      ResultTab(j).sectpriority := CondTab(indx).sectpriority;
      -- прямой договор
      ResultTab(j).condisn := CondTab(indx).condisn;
      ResultTab(j).agrisn := CondTab(indx).agrisn;
      ResultTab(j).agrdatebeg := CondTab(indx).agrdatebeg;
      ResultTab(j).agrdateend := CondTab(indx).agrdateend;
      ResultTab(j).agrcurrisn := CondTab(indx).agrcurrisn;
      ResultTab(j).agrclassisn := CondTab(indx).agrclassisn;
      ResultTab(j).buhdeptisn := CondTab(indx).buhdeptisn;
      ResultTab(j).rptgroupisn := CondTab(indx).rptgroupisn; -- EGAO 27.11.2012

      -- условие прямого договора
      ResultTab(j).premiumsum := CondTab(indx).premiumsum;
      ResultTab(j).datebeg := CondTab(indx).datebeg;
      ResultTab(j).dateend := CondTab(indx).dateend;
      ResultTab(j).virtualdatebeg := CondTab(indx).virtualdatebeg;
      ResultTab(j).virtualdateend := CondTab(indx).virtualdateend;
      ResultTab(j).lengthtype := CondTab(indx).lengthtype;
      ResultTab(j).gn := CondTab(indx).gn;
      ResultTab(j).sisum := CondTab(indx).sisum;
      ResultTab(j).sicurrdate := CondTab(indx).sicurrdate;
      -- начисленная премия (брутто_премия)
      ResultTab(j).buhamountnative := CondTab(indx).buhamountnative;
      ResultTab(j).buhamountagr := CondTab(indx).buhamountagr;
      ResultTab(j).buhamountusd := CondTab(indx).buhamountusd;
      ResultTab(j).buhamount := CondTab(indx).buhamount;
      ResultTab(j).condbuhamount := CondTab(indx).condbuhamount;
      ResultTab(j).condbuhamountagr := CondTab(indx).condbuhamountagr;
      -- рассчитанные показатели
      ResultTab(j).reprem := vRePrem; -- вклад_в_исх_премию
      ResultTab(j).repremprev := SectTab(to_char(CondTab(indx).sectisn)).PremPrevAmount; -- исх_fac
      ResultTab(j).coveragePremiumSharepc:= CoverageRec.Premium; -- коэффициент покрытия по времени
      -- вспомогательные поля
      ResultTab(j).sectduration := SectTab(to_char(CondTab(indx).sectisn)).Duration;
      ResultTab(j).sectremainder:= SectTab(to_char(CondTab(indx).sectisn)).Remainder;
      ResultTab(j).sectprorata := SectTab(to_char(CondTab(indx).sectisn)).Remainder/SectTab(to_char(CondTab(indx).sectisn)).Duration;
      ResultTab(j).shareprem := SectTab(to_char(CondTab(indx).sectisn)).PremShare;
      ResultTab(j).sharepremprev := SectTab(to_char(CondTab(indx).sectisn)).PremShare_t;
      ResultTab(j).sectnachprem := SectTab(to_char(CondTab(indx).sectisn)).NachPrem; -- нач_пермия_по_секции_без_восстановительной_до_отч_даты
      ResultTab(j).sectepi := SectTab(to_char(CondTab(indx).sectisn)).vEPI;
      ResultTab(j).sectemptyratelayercnt := SectTab(to_char(CondTab(indx).sectisn)).EmptyRateLayerCnt;
      ResultTab(j).sectdepospremsum := SectTab(to_char(CondTab(indx).sectisn)).DepospremSum;
      ResultTab(j).sectreinsuranceagrbuhamount := SectTab(to_char(CondTab(indx).sectisn)).ReinsuranceAgrBuhAmount;
      --ResultTab(j).sectlessthanayearagrcnt := SectTab(to_char(CondTab(indx).sectisn)).LessThanAYearAgcCnt;
      ResultTab(j).SectEmptyDeposPremLayerCnt := SectTab(to_char(CondTab(indx).sectisn)).EmptyDeposPremLayerCnt;

      IF MOD(j,1000) = 0 THEN
        FORALL indx IN ResultTab.FIRST .. ResultTab.LAST
        INSERT INTO rnp_re_msfo_premium
        VALUES ResultTab(indx);
        ResultTab.delete;
      END IF;

      IF layer_cur%ISOPEN THEN CLOSE layer_cur; END IF;
      IF cond2sect_cur%ISOPEN THEN CLOSE cond2sect_cur; END IF;
    END LOOP;

    FORALL indx IN ResultTab.FIRST .. ResultTab.LAST
    INSERT INTO rnp_re_msfo_premium
    VALUES ResultTab(indx);

    COMMIT;
  END;

  PROCEDURE make_rnp_re_msfo_rnp(pLoadIsn IN NUMBER)
  IS
    vDateRep DATE := GetDateRep(pLoadIsn);

    vMinIsn number:=0;
    vMaxIsn number;
    vSql varchar2(4000);
    SesId Number;
    vLoadObjCnt number:=100000;
    vCnt number:=0;
    vPart VARCHAR2(30);
  BEGIN
    IF vDateRep IS NULL THEN
      raise_application_error(en_invalid_daterep,'Invalid daterep');
    END IF;
    SesId:=Parallel_Tasks.createnewsession();

    vPart:=init_partition_by_key(pTableName => 'storages.rnp_re_msfo_rnp',pKey => pLoadIsn,pCompress => 1);
    dbms_lock.sleep(20);
    replog_i (pLoadIsn, 'LoadRNPNEW', 'make_rnp_re_msfo_rnp',pAction=>'Begin');

    LOOP

      SELECT MAX (b.agrisn)
      INTO  vMaxIsn
      FROM (
            SELECT --+ index (b x_rnp_msfo_agr)
                   b.agrisn
            FROM rnp_msfo b
            WHERE b.agrisn > vMinIsn
              AND b.loadisn=pLoadisn
              AND rownum <= vLoadObjCnt) b;

      EXIT WHEN vMaxIsn IS NULL;

      vSql:= 'DECLARE
                vLoadIsn number :='||pLoadIsn||';
                vMinIsn number :='||vMinIsn||';
                vMaxIsn number :='||vMaxIsn||';
                vCnt    number :='||vCnt||';
              BEGIN
                DBMS_APPLICATION_INFO.SET_MODULE(''rnp_re_msfo_rnp_by_isn'',''Precess#''||vCNT);
                storages.report_rnp_new.make_rnp_re_msfo_rnp_by_isn(pLoadIsn => vLoadIsn,
                                        pminisn => vMinIsn,
                                        pmaxisn => vMaxIsn);
              END;';

      System.Parallel_Tasks.processtask(sesid,vsql);
      vCnt:=vCnt+1;
      vMinIsn:=vMaxIsn;

      DBMS_APPLICATION_INFO.set_module('rnp_re_msfo','Applied: '||vCnt*vLoadObjCnt);

    END LOOP;
    -- ждем, пока завершатся все джобы
    Parallel_Tasks.endsession(sesid);
    replog_i (pLoadIsn, 'LoadRNPNEW', 'make_rnp_re_msfo_rnp',pAction=>'End');
  END;

  PROCEDURE make_rnp_re_msfo_rnp_by_isn(pLoadIsn IN NUMBER, pMinIsn IN NUMBER, pMaxIsn IN NUMBER)
  IS
    vDateRep DATE := GetDateRep(pLoadIsn);
    vXLShare NUMBER;
    vCondIsn       NUMBER := -1;
    vAgrIsn        NUMBER := -1;
    vVirtualDateBeg DATE := to_date('01.01.1900','dd.mm.yyyy');
    vVirtualDateEnd DATE := to_date('01.01.1900','dd.mm.yyyy');
    vIndex         BINARY_INTEGER;
    vPrioritySum   NUMBER := 0;
    vLayerShare    NUMBER := 0;
    vLongCondPremPc NUMBER;
    vSectIsn NUMBER := -999;

    i NUMBER := gcc2.gcc2(1,LocalCurr,DollarCurr,SYSDATE);
    j NUMBER := 0;
    vIsn NUMBER := 0;

    vReRNP    NUMBER;
    vReRNP_t  NUMBER;
    vRePrem   NUMBER;
    vRePrem_t NUMBER;

    TYPE TCond IS TABLE OF v_rnp_re_msfo_rnp%ROWTYPE;

    CondTab TCond;

    TYPE TPrevSectElement IS RECORD(
      SharePc NUMBER,
      RNPValue NUMBER
    );

    PrevSectElement TPrevSectElement;

    TYPE TPrevSectTab IS TABLE OF TPrevSectElement INDEX BY BINARY_INTEGER;

    PrevSectTab TPrevSectTab;

    TYPE TSectRec IS RECORD(
      Isn NUMBER,
      Duration NUMBER,
      Remainder NUMBER,
      ProRata NUMBER,
      BruttoRNP NUMBER,
      RNPShare NUMBER,
      RNPShare_t NUMBER,
      RNPPrevAmount NUMBER,
      NachPrem NUMBER,
      LessThanAYearAgcCnt NUMBER,
      DepospremSum NUMBER,
      EmptyRateLayerCnt NUMBER,
      vEPI NUMBER,
      EmptyDeposPremLayerCnt NUMBER,
      ReinsuranceAgrBuhAmount NUMBER, -- брутто-премия всех условий прямых/входящих договоров, перестрахованных указанной секцией
      ReinsuranceAgrEarnedAmount NUMBER, -- заработанная премия по всем перестрахованным условиям за время действия секции
      SharePc NUMBER -- сумма долей участников (размещение секции)
    );

    SectRec TSectRec;

    TYPE TSectTab IS TABLE OF TSectRec INDEX BY VARCHAR2(100);

    SectTab TSectTab;


    TYPE TCoverageRec IS RECORD(
      Premium NUMBER, -- покрытие по времени доначисление EGAO 03.07.2012
      Rnp NUMBER, -- покрытие по времени РНП
      CondDateBeg DATE,
      CondDateEnd DATE,
      CondDuration NUMBER,
      CondRemainder NUMBER
    );

    CoverageRec TCoverageRec;

    CURSOR cond2sect_cur(agrisn_in NUMBER, condisn_in NUMBER, secttype_in IN VARCHAR2)
    IS
    SELECT MAX(s.secttype) AS secttype,
           MAX(s.datebeg) AS datebeg,
           MAX(s.currisn) AS currisn,
           NVL(MIN(cd.prioritysum),0) AS prioritysum,
           NVL(SUM(CASE WHEN pr.parentisn IS NULL THEN pr.sharepc END)/100,0) AS sharepc
    FROM (SELECT s.*,
                 rank() over (ORDER BY CASE secttype_in
                                         WHEN 'RX' THEN CASE s.secttype WHEN 'QS' THEN 1 WHEN 'XL' THEN 2 END
                                         WHEN 'QS' THEN CASE s.secttype WHEN 'RX' THEN 1 WHEN 'QS' THEN 2 WHEN 'XL' THEN 3  END
                                         WHEN 'SP' THEN CASE s.secttype WHEN 'RX' THEN 1 WHEN 'QS' THEN 2 WHEN 'XL' THEN 3  END
                                       END) AS rn
          FROM (SELECT --+ ordered use_nl ( x reagr agr ) index ( x X_REP_AGRRE_SECTISN )
                       x.sectisn,
                       x.secttype,
                       x.sectcurrisn AS currisn,
                       x.sdatebeg AS datebeg,
                       COUNT(CASE WHEN x.secttype='QS' THEN 1 END) over  (PARTITION BY x.secttype) AS qscnt,
                       COUNT(CASE WHEN x.secttype='XL' THEN 1 END) over  (PARTITION BY x.secttype) AS xlcnt,
                       COUNT(CASE WHEN x.secttype='RX' THEN 1 END) over  (PARTITION BY x.secttype) AS rxcnt
                 FROM rep_agrre x,
                      agreement reagr,
                      agreement agr
                 WHERE x.agrisn=agrisn_in
                   AND (x.condisn IS NULL OR x.condisn=condisn_in)
                   AND reagr.isn=x.reisn
                   --EGAO 24.08.2011 AND ((x.secttype IN ('QS','XL') AND reagr.classisn=9018) OR (x.secttype='RX'))
                   AND agr.isn=x.agrisn
                   AND CASE
                         WHEN reagr.datebase='C' AND vDateRep BETWEEN x.sdatebeg AND x.sdateend  THEN 1
                         WHEN reagr.datebase='I' AND agr.datebeg BETWEEN x.sdatebeg AND x.sdateend THEN 1
                         ELSE 0
                       END = 1
                   --{EGAO 24.08.2011
                   AND CASE secttype_in
                         WHEN 'RX' THEN CASE WHEN reagr.classisn=9018 AND x.secttype IN ('QS','XL') THEN 1 ELSE 0 END
                         WHEN 'QS' THEN CASE WHEN reagr.classisn=9058 AND x.secttype='RX' OR reagr.classisn=9018 AND x.secttype IN ('QS', 'XL') THEN 1 ELSE 0 END
                         WHEN 'SP' THEN CASE WHEN reagr.classisn=9058 AND x.secttype='RX' OR reagr.classisn=9018 AND x.secttype IN ('QS', 'XL') THEN 1 ELSE 0 END
                         ELSE 0
                       END = 1
                   --}

                ) s
          WHERE  qscnt>0 OR xlcnt=1 OR rxcnt>0 -- EGAO 17.05.2011 WHERE  (s.secttype='QS' AND qscnt>0) OR (s.secttype='XL' AND xlcnt=1) OR (rxcnt>0)
         ) s,
         recond cd,
         resubjperiod pr
    WHERE cd.sectisn=s.sectisn AND pr.condisn=cd.isn AND rn=1;

    cond2sect_rec cond2sect_cur%ROWTYPE;

    CURSOR layer_cur(sectisn_in NUMBER)
    IS
    --{EGAO 14.08.2013 т.к. закомментированный код не соответствует ТЗ
    /*SELECT cd.isn,
           MAX(decode(cd.rate,NULL,1,0)) AS rateisnull,
           MAX(decode(cd.limitsum,NULL,1,0)) AS limitsumisnull,
           NVL(MAX(cd.rate)/100,0) AS rate,
           NVL(MAX(cd.limitsum),0) AS limitsum,
           NVL(MAX(cd.prioritysum),0) AS prioritysum,
           NVL(max(cd.depospremsum),0) AS depospremsum,
           NVL(SUM(CASE WHEN pr.parentisn IS NULL THEN pr.sharepc END)/100,0) AS sharepc
    FROM recond cd,
         resubjperiod pr
    WHERE cd.sectisn=sectisn_in
      AND pr.condisn=cd.isn
    GROUP BY cd.isn;*/
    SELECT a.condisn, 
           MAX(decode(a.rate,NULL,1,0)) AS rateisnull,
           MAX(decode(a.limitsum,NULL,1,0)) AS limitsumisnull,
           NVL(MAX(a.rate)/100,0) AS rate,
           NVL(MAX(a.limitsum),0) AS limitsum,
           NVL(MAX(a.prioritysum),0) AS prioritysum,
           NVL(max(a.depospremsum),0) AS depospremsum,
           SUM(CASE WHEN nvl(a.condsharepc,0)=0 THEN 0 ELSE a.sharepc*a.parentsharepc/a.condsharepc END)/100 AS sharepc
    FROM (
           SELECT --+ ordered use_nl ( a cd )
                  a.condisn,
                  a.parentsharepc,
                  a.sharepc,
                  SUM(a.sharepc) over (PARTITION BY a.condisn, a.parentisn) AS condsharepc,
                  cd.rate, -- Премия лейера (в recond в %, здесь - доля)
                  cd.prioritysum, -- Приоритет лейера
                  cd.limitsum, -- Лимит лейера
                  cd.depospremsum-- Депозитная премия лейера
           FROM (
                 SELECT a.sectisn, -- секция
                        a.condisn, -- лейер
                        nvl(a.sharepc,0) AS sharepc, -- доля размещения участника
                        connect_by_root(nvl(a.sharepc,0)) AS parentsharepc, -- доля размещения брокера
                        connect_by_root(a.isn) AS parentisn
                 FROM resubjperiod a
                 WHERE CONNECT_BY_ISLEAF=1 AND nvl(a.sharepc,0)<>0
                 CONNECT BY PRIOR a.isn=a.parentisn
                 START WITH a.parentisn IS NULL AND nvl(a.sharepc,0)<>0 AND a.sectisn=sectisn_in 
                ) a,
                recond cd
           WHERE cd.isn=a.condisn
             AND cd.isn NOT IN (119901171303, 111824735603) -- EGAO 11.01.2012 Лейеры заведены некорректно, исправлению в АИС не подлежат. Исключаем их обработку в коде
         ) a
    GROUP BY a.condisn;
    --}EGAO 14.08.2013

    layer_rec layer_cur%ROWTYPE;

    TYPE TResultTab IS TABLE OF rnp_re_msfo_rnp%ROWTYPE INDEX BY BINARY_INTEGER;

    ResultTab TResultTab;
  BEGIN

    IF vDateRep IS NULL THEN
      raise_application_error(en_invalid_daterep,'Invalid daterep');
    END IF;


    pparam.Clear;
    pparam.SetParamN('MinAgrIsn', pMinIsn);
    pparam.SetParamN('MaxAgrIsn', pMaxIsn);
    pparam.SetParamN('LoadIsn', pLoadIsn);
    pparam.SetParamD('DateRep', vDateRep);

    DELETE FROM tt_rnp_re_msfo_cond_rnp;

    INSERT INTO tt_rnp_re_msfo_cond_rnp (
      condisn, agrisn, datebeg, dateend, virtualdatebeg, virtualdateend, buhdeptisn,
      bruttornp, condbruttornp, agrcurrisn, agrclassisn,
      agrdatebeg, agrdateend,rptgroupisn, sagroup, isrevaluation, rptclass, bruttornpagr, sharepc
    )
    SELECT condisn, agrisn, datebeg, dateend, virtualdatebeg, virtualdateend, buhdeptisn,
           bruttornp, condbruttornp, agrcurrisn, agrclassisn,
           agrdatebeg, agrdateend,rptgroupisn, sagroup, isrevaluation, rptclass, bruttornpagr,
           sharepc -- EGAO 06.12.2011
    FROM v_rnp_re_msfo_cond_rnp;

    SELECT a.*
    BULK COLLECT INTO CondTab
    FROM v_rnp_re_msfo_rnp a
    ORDER BY a.agrisn, a.condisn, a.virtualdatebeg, a.virtualdateend, a.sectpriority, a.sectisn;

    IF CondTab.count=0 THEN
      RETURN;
    END IF;


    FOR indx IN CondTab.first..CondTab.last  LOOP
      DBMS_APPLICATION_INFO.SET_MODULE('rnp_re_msfo_rnp '||CondTab(indx).condisn,CondTab(indx).agrisn||'-'||CondTab(indx).sectisn);
      vLayerShare := 0;
      vReRNP := 0;
      vReRNP_t := 0;
      vRePrem := 0;
      vRePrem_t := 0;
      vPrioritySum := 0;

      IF CondTab(indx).agrisn<>vAgrIsn OR
         CondTab(indx).condisn<>vCondIsn OR
         CondTab(indx).virtualdatebeg<>vVirtualDateBeg OR
         CondTab(indx).virtualdateend<>vVirtualDateEnd OR
         CondTab(indx).sectisn<>vSectIsn THEN

        IF CondTab(indx).agrisn<>vAgrIsn OR
           CondTab(indx).condisn<>vCondIsn OR
           CondTab(indx).virtualdatebeg<>vVirtualDateBeg OR
           CondTab(indx).virtualdateend<>vVirtualDateEnd THEN
          PrevSectTab.DELETE;
          vLongCondPremPc := to_number(NULL);
        END IF;

        SectRec := NULL;
        SectRec.RNPShare := 0;
        SectRec.RNPShare_t := 0;
        SectRec.RNPPrevAmount := 0;

        IF CondTab(indx).agrisn<>vAgrIsn AND CondTab(indx).reclassisn=-1
           AND SectTab.exists(to_char(CondTab(indx).sectisn)) THEN
          SectTab.delete(to_char(CondTab(indx).sectisn));
        END IF;
        IF NOT SectTab.exists(to_char(CondTab(indx).sectisn)) THEN
          --dbms_output.put_line(CondTab(indx).sectisn);
          IF CondTab(indx).reclassisn = -1 THEN
            SELECT --+ index ( a X_REPBUHBODY_AGR ) no_merge ( h ) use_hash ( a h )
                   NVL(-SUM(CASE a.currisn
                              WHEN DollarCurr THEN a.amount
                              ELSE gcc2.gcc2(a.amount,a.currisn,DollarCurr, vDateRep)
                            END),0)
            INTO SectRec.NachPrem
            FROM repbuhbody a,
                 (SELECT h.isn FROM buhsubacc h WHERE h.id LIKE '924%' OR h.id LIKE '914%' OR h.id LIKE '916%') h
            WHERE a.agrisn=CondTab(indx).agrisn
              AND h.ISN=a.subaccisn
              AND a.dateval<=vDateRep
              AND a.dsclassisn=414
              AND a.dsclassisn2<>2265208403;

          ELSE
            SELECT --+ index ( a X_REPBUHRE2RESECTION_SECT ) no_merge ( h ) use_hash ( a h )
                     NVL(-SUM(CASE a.buhcurrisn
                                WHEN DollarCurr THEN a.amount
                                ELSE gcc2.gcc2(a.amount,a.buhcurrisn,DollarCurr, vDateRep)
                              END),0)
              INTO SectRec.NachPrem
              FROM repbuhre2resection_new a /*EGAO repbuhre2resection a*/,
                   (SELECT h.isn FROM buhsubacc h WHERE h.id LIKE '924%' OR h.id LIKE '914%' OR h.id LIKE '916%') h
              WHERE a.sectisn=CondTab(indx).sectisn
                AND h.ISN=a.subaccisn
                AND a.dateval<=vDateRep
                AND a.dsclassisn=414;

            SectRec.Duration :=greatest(1, (CondTab(indx).sectdateend-CondTab(indx).sectdatebeg+1));
            SectRec.Remainder:=CASE
                                 WHEN vDateRep>CondTab(indx).sectdatebeg THEN greatest(0, (CondTab(indx).sectdateend-vDateRep))
                                 ELSE SectRec.Duration
                               END;
            SELECT SUM(CASE CondTab(indx).sectcurrisn
                       WHEN DollarCurr THEN depospremsum
                       ELSE gcc2.gcc2(depospremsum,CondTab(indx).sectcurrisn,DollarCurr,vDateRep)
                     END*sharepc),
                     COUNT(DECODE(rate,NULL,1)),
                     MAX(epi),
                     COUNT(DECODE(depospremsum,NULL,1)),
                     NVL(MAX(sharepc),0) AS sharepc
            INTO SectRec.DepospremSum, SectRec.EmptyRateLayerCnt, SectRec.vEPI, SectRec.EmptyDeposPremLayerCnt, SectRec.SharePc
            FROM (
                  SELECT cd.isn,
                         MAX(cd.depospremsum) AS depospremsum,
                         NVL(SUM(CASE WHEN pr.parentisn IS NULL THEN pr.sharepc END)/100,0) AS sharepc,
                         MAX(cd.rate/100) AS rate,
                         MAX(cd.epi) AS epi
                  FROM recond cd,
                       resubjperiod pr
                  WHERE cd.sectisn=CondTab(indx).sectisn
                    AND pr.condisn=cd.isn
                  GROUP BY cd.isn
                 );
            IF CondTab(indx).redatebase='I' THEN
              SELECT COUNT(1)
              INTO SectRec.LessThanAYearAgcCnt
              FROM dual
              WHERE EXISTS (SELECT --+ index ( x X_AGRX_SECT ) ordered use_nl ( x ra )
                                   'x'
                            FROM agrx x, repagr ra
                            WHERE x.sectisn=CondTab(indx).sectisn
                            AND ra.agrisn=x.agrisn
                            AND months_between(vDateRep, trunc(ra.dateend))<12
                           );
            END IF;
            IF CondTab(indx).secttype IN ('XL','SL') AND SectRec.EmptyRateLayerCnt>0 AND NVL(SectRec.vEPI, 0)=0 AND CondTab(indx).redatebase='I' THEN
              --{ EGAO 22.05.2012 оптимизация запроса (isn секции на которой старый вариант затыкался - 156176144603
              /*SELECT --+ ordered use_nl ( x bc ) index( bc X_REPBUH2COND_AGRISN ) no_merge ( x )
                     NVL(SUM(decode(bc.buhcurrisn,CondTab(indx).sectcurrisn, bc.amount,gcc2.gcc2(bc.amount,bc.buhcurrisn,CondTab(indx).sectcurrisn,bc.dateval))*x.shareneorig),0)
              INTO SectRec.ReinsuranceAgrBuhAmount
              FROM
                   (SELECT --+ index ( x X_REP_AGRRE_SECTISN )
                           DISTINCT
                           x.agrisn,
                           x.condisn,
                           CASE x.reclassisn
                             WHEN 9018 THEN 1
                             WHEN 9058 THEN nvl(x.shareneorig/100, 1)
                           END AS shareneorig
                    FROM rep_agrre x
                    WHERE x.sectisn=CondTab(indx).sectisn
                   ) x,
                   repbuh2cond bc
              WHERE bc.agrisn=x.agrisn
                AND (x.condisn IS NULL OR bc.condisn=x.condisn)
                AND bc.statcode IN (38, 34, 221, 241)
                AND bc.sagroup IN (1, 3)
                AND bc.dateval<= vDateRep;*/
              WITH x AS (SELECT --+ index ( x X_REP_AGRRE_SECTISN )
                                DISTINCT x.agrisn, x.condisn,
                                         CASE x.reclassisn
                                           WHEN 9018 THEN 1
                                           WHEN 9058 THEN nvl(x.shareneorig/100, 1)
                                         END AS shareneorig
                         FROM rep_agrre x
                         WHERE x.sectisn=CondTab(indx).sectisn
                        )
              SELECT NVL(SUM(decode(bc.buhcurrisn,CondTab(indx).sectcurrisn, bc.amount,gcc2.gcc2(bc.amount,bc.buhcurrisn,CondTab(indx).sectcurrisn,bc.dateval))*bc.shareneorig),0)
              INTO SectRec.ReinsuranceAgrBuhAmount
              FROM (SELECT --+ index ( bc X_REPBUH2COND_COND ) ordered use_nl ( x bc )
                           bc.*, x.shareneorig
                    FROM x, repbuh2cond bc
                    WHERE bc.agrisn=x.agrisn
                      AND x.condisn IS NOT NULL 
                      AND bc.condisn=x.condisn
                      AND bc.statcode IN (38, 34, 221, 241)
                      AND bc.sagroup IN (1, 3)
                      AND bc.dateval<= vDateRep
                      UNION ALL
                    SELECT --+ index ( bc X_REPBUH2COND_AGRISN ) ordered use_nl ( x bc )
                           bc.*, x.shareneorig
                    FROM x, repbuh2cond bc
                    WHERE bc.agrisn=x.agrisn
                      AND x.condisn IS NULL
                      AND bc.statcode IN (38, 34, 221, 241)
                      AND bc.sagroup IN (1, 3)
                      AND bc.dateval<= vDateRep  
                    ) bc;   
              --}
            END IF;
            IF CondTab(indx).secttype IN ('XL','SL') AND SectRec.EmptyRateLayerCnt>0 AND CondTab(indx).redatebase='C' AND CondTab(indx).reclassisn=AgrFacultType THEN
              SELECT --+ index (a X_RNP_RE_MSFO_XL_SECTION_SECT )
                     NVL(SUM(a.buhamountusd*a.coverageprem),0)
              INTO SectRec.ReinsuranceAgrEarnedAmount
              FROM rnp_re_msfo_xl_section a
              WHERE a.loadisn=pLoadIsn
               AND a.sectisn=CondTab(indx).sectisn;
            END IF;
          END IF;
        ELSE
          SectRec.NachPrem := SectTab(to_char(CondTab(indx).sectisn)).NachPrem;
          SectRec.Duration := SectTab(to_char(CondTab(indx).sectisn)).Duration;
          SectRec.Remainder:= SectTab(to_char(CondTab(indx).sectisn)).Remainder;
          SectRec.DepospremSum := SectTab(to_char(CondTab(indx).sectisn)).DepospremSum;
          SectRec.EmptyRateLayerCnt := SectTab(to_char(CondTab(indx).sectisn)).EmptyRateLayerCnt;
          SectRec.vEPI := SectTab(to_char(CondTab(indx).sectisn)).vEPI;
          SectRec.EmptyDeposPremLayerCnt := SectTab(to_char(CondTab(indx).sectisn)).EmptyDeposPremLayerCnt;
          SectRec.LessThanAYearAgcCnt := SectTab(to_char(CondTab(indx).sectisn)).LessThanAYearAgcCnt;
          --{EGAO 17.10.2011
          SectRec.ReinsuranceAgrBuhAmount := SectTab(to_char(CondTab(indx).sectisn)).ReinsuranceAgrBuhAmount;
          SectRec.ReinsuranceAgrEarnedAmount := SectTab(to_char(CondTab(indx).sectisn)).ReinsuranceAgrEarnedAmount;
          --}
          --{EGAO 14.08.2013
          SectRec.SharePc := SectTab(to_char(CondTab(indx).sectisn)).SharePc;
          --}EGAO 14.08.2013
        END IF;

        CoverageRec := NULL;

        vIndex := PrevSectTab.first;
        WHILE CondTab(indx).sectpriority > vIndex LOOP
          SectRec.RNPPrevAmount  := SectRec.RNPPrevAmount + PrevSectTab(vIndex).rnpvalue;
          vIndex := PrevSectTab.next(vIndex);
        END LOOP;

        IF CondTab(indx).reclassisn = -1 THEN -- Перестрахование по схеме "Участники"
          SELECT --+ index ( rl X_AGRROLE_AGR )
                 NVL(SUM(decode(rl.sumclassisn2,414, rl.sharepc,8133016,rl.sharepc))/100 , 0) sharepc
          INTO SectRec.RNPShare
          FROM agrrole rl
          WHERE rl.agrisn=CondTab(indx).agrisn
            AND rl.orderno>0
            AND rl.classisn=435
            AND rl.sumclassisn=414
            AND rl.sharepc<>0
            AND rl.calcflg='Y';
          SectRec.RNPShare_t := SectRec.RNPShare;
        ELSE
          IF CondTab(indx).redatebase='I' THEN
            IF CondTab(indx).virtualdatebeg BETWEEN CondTab(indx).sectdatebeg AND CondTab(indx).sectdateend 
              OR CondTab(indx).agrdatebeg BETWEEN CondTab(indx).sectdatebeg AND CondTab(indx).sectdateend -- EGAO 15.07.2013 52198541303
              THEN
              CoverageRec.Rnp := 1;
              CoverageRec.Premium := 1; -- EGAO 03.07.2012
            ELSE
              CoverageRec.Rnp := 0;
              CoverageRec.Premium := 0; -- EGAO 03.07.2012
            END IF;
          ELSIF CondTab(indx).redatebase='C' THEN
            CoverageRec.CondDateBeg := CondTab(indx).virtualdatebeg;
            IF CondTab(indx).lengthtype = 'L' THEN
              IF vLongCondPremPc IS NULL THEN
                SELECT CASE WHEN premiumsum=0 THEN 0 ELSE least(1, paidsum/premiumsum) END
                INTO vLongCondPremPc
                FROM (
                      SELECT --+ Index (x X_REP_LONGAGR_PAYSUM_COND)
                             NVL(SUM(x.paidsum),0) AS paidsum,NVL(MAX(x.premiumsum),0) AS premiumsum
                      FROM rep_longagr_paysum x
                      WHERE x.condisn=CondTab(indx).condisn
                        AND x.loadisn=pLoadIsn
                        AND x.dateval<=vDateRep
                        AND x.repclassisn=1 -- EGAO 29.05.2012
                     );
              END IF;
              IF vLongCondPremPc >0 AND vLongCondPremPc < 1 THEN
                CoverageRec.CondDuration := (CondTab(indx).virtualdateend-CondTab(indx).virtualdatebeg+1)*vLongCondPremPc;
                CoverageRec.CondDateEnd := CoverageRec.CondDateBeg+CoverageRec.CondDuration-1;
              ELSE
                CoverageRec.CondDateEnd := CondTab(indx).virtualdateend;
              END IF;
            ELSE
              CoverageRec.CondDateEnd := CondTab(indx).virtualdateend;
            END IF;
            CoverageRec.CondDuration :=greatest(1, (CoverageRec.CondDateEnd-CoverageRec.CondDateBeg+1));
            CoverageRec.CondRemainder:=CASE
                                         WHEN vDateRep>CoverageRec.CondDateBeg THEN greatest(0, (CoverageRec.CondDateEnd-vDateRep))
                                         ELSE CoverageRec.CondDuration
                                       END;
            CoverageRec.Rnp := CASE
                                 WHEN CoverageRec.CondRemainder=0 THEN 0
                                 ELSE least(SectRec.Remainder, CoverageRec.CondRemainder)/CoverageRec.CondRemainder
                               END;
            --{ EGAO 03.07.2012
            CoverageRec.Premium := greatest(0, (least(CondTab(indx).sectdateend,CoverageRec.CondDateEnd)-greatest(CondTab(indx).sectdatebeg,CoverageRec.CondDateBeg)+1)/CoverageRec.CondDuration);
            --}                   
          END IF;

          IF CondTab(indx).secttype='QS' THEN
            OPEN cond2sect_cur(CondTab(indx).agrisn, CondTab(indx).condisn,  CondTab(indx).sectisn);
            FETCH cond2sect_cur INTO cond2sect_rec;
            vPrioritySum := CASE
                              WHEN CondTab(indx).sectcurrisn=cond2sect_rec.currisn THEN cond2sect_rec.prioritysum
                              ELSE gcc2.GCC2(cond2sect_rec.prioritysum, cond2sect_rec.currisn, CondTab(indx).sectcurrisn, CondTab(indx).sicurrdate)
                            END;

            OPEN layer_cur(CondTab(indx).sectisn);
            FETCH layer_cur INTO layer_rec;
            IF layer_cur%FOUND THEN

              IF layer_rec.rateisnull =1 OR CondTab(indx).shareneorig<>1 THEN
                IF layer_rec.limitsumisnull = 1 OR CondTab(indx).reclassisn = AgrFacultType THEN
                  vLayerShare := 1;
                ELSE
                  IF cond2sect_rec.SectType='RX' THEN
                    vLayerShare := CASE
                                     WHEN vPrioritySum=0 THEN 1
                                     ELSE least(1, layer_rec.limitsum/vPrioritySum)
                                   END;
                  ELSIF cond2sect_rec.SectType='QS' THEN
                    vLayerShare :=  CASE
                                      WHEN CondTab(indx).sisum=0  THEN 1
                                      WHEN cond2sect_rec.sharepc>=1 THEN 0
                                      ELSE least(1,(layer_rec.limitsum/(CondTab(indx).sisum*(1-cond2sect_rec.sharepc))))
                                    END;
                  ELSIF cond2sect_rec.SectType='XL' THEN
                    vLayerShare := CASE
                                     WHEN vPrioritySum=0 THEN 1
                                     ELSE least(1, layer_rec.limitsum/vPrioritySum)
                                   END;
                  ELSE
                    vLayerShare := CASE
                                     WHEN CondTab(indx).sisum=0 THEN 1
                                     ELSE least(1, layer_rec.limitsum/CondTab(indx).sisum)
                                   END;
                  END IF;
                END IF;
                SectRec.RNPShare := vLayerShare*layer_rec.sharepc*CondTab(indx).shareneorig*CoverageRec.Rnp;
              ELSE
                --{ EGAO 03.07.2012
                --SectRec.RNPShare  := (1- layer_rec.rate)*CoverageRec.Rnp;
                IF CoverageRec.Premium=0 THEN 
                  SectRec.RNPShare := 0;
                ELSE  
                  SectRec.RNPShare  := (1- layer_rec.rate)*CoverageRec.Rnp/CoverageRec.Premium;
                END IF;  
                --}
              END IF;
              SectRec.RNPShare_t := SectRec.RNPShare;
            END IF;

          ELSIF CondTab(indx).secttype='SP' THEN
            OPEN cond2sect_cur(CondTab(indx).agrisn, CondTab(indx).condisn, CondTab(indx).secttype);
            FETCH cond2sect_cur INTO cond2sect_rec;

            vPrioritySum := CASE
                              WHEN CondTab(indx).sectcurrisn=cond2sect_rec.currisn THEN cond2sect_rec.prioritysum
                              ELSE gcc2.GCC2(cond2sect_rec.prioritysum, cond2sect_rec.currisn, CondTab(indx).sectcurrisn, CondTab(indx).sicurrdate)
                            END;
            OPEN layer_cur(CondTab(indx).sectisn);
            FETCH layer_cur INTO layer_rec;
            IF layer_cur%FOUND THEN
              IF layer_rec.limitsumisnull = 1 THEN -- EGAO 23.06.2010 (дополнение в ТЗ, сделанное Дмитревской, после случая с договором УМС01/05, у которого не был указан лимит секции)
                vLayerShare := CASE
                                 WHEN CondTab(indx).sisum=0 THEN 1
                                 ELSE greatest((CondTab(indx).sisum-layer_rec.prioritysum)/CondTab(indx).sisum,0)
                               END;
              ELSE
                IF CondTab(indx).reclassisn = AgrObligType AND cond2sect_rec.SectType='RX' THEN
                  vLayerShare := CASE
                                   WHEN vPrioritySum=0 THEN 0
                                   ELSE least(greatest((vPrioritySum-layer_rec.prioritysum)/vPrioritySum,0),(layer_rec.limitsum-layer_rec.prioritysum)/vPrioritySum)
                                 END;
                ELSIF CondTab(indx).reclassisn = AgrObligType AND cond2sect_rec.SectType='QS' THEN
                  vLayerShare := CASE
                                   WHEN CondTab(indx).sisum=0 OR cond2sect_rec.sharepc>=1  THEN 0
                                   ELSE least(greatest((CondTab(indx).sisum*(1-cond2sect_rec.sharepc)-layer_rec.prioritysum)/(CondTab(indx).sisum*(1-cond2sect_rec.sharepc)),0
                                                      ),(layer_rec.limitsum-layer_rec.prioritysum)/(CondTab(indx).sisum*(1-cond2sect_rec.sharepc)))
                                 END;
                ELSIF CondTab(indx).reclassisn = AgrObligType AND cond2sect_rec.SectType='XL' THEN
                  vLayerShare := CASE
                                   WHEN vPrioritySum=0 THEN 0
                                   ELSE least(greatest((vPrioritySum-layer_rec.prioritysum)/vPrioritySum,0),(layer_rec.limitsum-layer_rec.prioritysum)/vPrioritySum)
                                 END;
                ELSE
                  vLayerShare := CASE
                                   WHEN CondTab(indx).sisum=0 THEN 1
                                   ELSE least(greatest((CondTab(indx).sisum-layer_rec.prioritysum)/CondTab(indx).sisum,0),(layer_rec.limitsum-layer_rec.prioritysum)/CondTab(indx).sisum)
                                 END;
                END IF;
              END IF;
              SectRec.RNPShare := vLayerShare*layer_rec.sharepc*CondTab(indx).shareneorig*CoverageRec.Rnp;
              SectRec.RNPShare_t := SectRec.RNPShare;
            END IF;

          ELSIF CondTab(indx).secttype IN ('XL','SL') THEN
            IF SectRec.EmptyRateLayerCnt>0 AND NVL(SectRec.vEPI,0)<>0 AND
               (CondTab(indx).redatebase='I'  OR (CondTab(indx).redatebase='C' AND CondTab(indx).reclassisn=AgrObligType)) THEN
              OPEN layer_cur(CondTab(indx).sectisn);
              LOOP
                FETCH layer_cur INTO layer_rec;
                EXIT WHEN layer_cur%NOTFOUND;
                vLayerShare := vLayerShare + layer_rec.depospremsum*layer_rec.sharepc;
              END LOOP;
              SectRec.RNPShare := CondTab(indx).shareneorig*vLayerShare/SectRec.vEPI*CoverageRec.Rnp;
            ELSIF SectRec.EmptyRateLayerCnt>0 AND NVL(SectRec.vEPI, 0)=0 AND CondTab(indx).redatebase='I'  THEN
              /*SELECT --+ ordered use_nl ( x bc ) index( bc X_REPBUH2COND_AGRISN ) no_merge ( x )
                     NVL(SUM(decode(bc.buhcurrisn,CondTab(indx).sectcurrisn, amount,gcc2.gcc2(bc.amount,bc.buhcurrisn,CondTab(indx).sectcurrisn,bc.dateval))*x.shareneorig),0)
              INTO SectRec.ReinsuranceAgrBuhAmount
              FROM
                   (SELECT --+ ordered use_nl ( x rsk reag ) index ( x X_AGRX_SECT) ondex ( rsk X_AGRRISK_AGR )
                           DISTINCT
                           x.agrisn,
                           x.objisn,
                           CASE reag.classisn
                             WHEN 9018 THEN 1
                             WHEN 9058 THEN nvl(x.shareneorig/100, 1)
                           END AS shareneorig,
                           rsk.classisn AS riskclassisn
                    FROM agrx x, agrrisk rsk, repagr reag
                    WHERE x.sectisn=CondTab(indx).sectisn
                      AND rsk.agrisn=x.agrisn
                      AND (x.riskisn=0 OR rsk.isn IN (SELECT b.isn FROM agrrisk b START WITH b.isn=x.riskisn CONNECT BY PRIOR b.isn=b.parentisn))
                      AND reag.agrisn=x.reisn
                   ) x,
                   repbuh2cond bc
              WHERE bc.agrisn=x.agrisn
                AND (x.objisn=0 OR bc.objisn=x.objisn)
                AND bc.riskclassisn=x.riskclassisn
                AND bc.statcode IN (38, 34, 221, 241)
                AND bc.sagroup IN (1, 3)
                AND bc.dateval<= vDateRep;*/

              IF SectRec.ReinsuranceAgrBuhAmount=0 THEN
                SectRec.RNPShare := 0;
              ELSE
                OPEN layer_cur(CondTab(indx).sectisn);
                LOOP
                  FETCH layer_cur INTO layer_rec;
                  EXIT WHEN layer_cur%NOTFOUND;
                  vLayerShare := vLayerShare + layer_rec.depospremsum*layer_rec.sharepc;
                END LOOP;
                SectRec.RNPShare := vLayerShare/SectRec.ReinsuranceAgrBuhAmount*CondTab(indx).shareneorig*CoverageRec.Rnp;
              END IF;
            --{ EGAO 17.10.2011
            ELSIF SectRec.EmptyRateLayerCnt>0 AND CondTab(indx).redatebase='C' AND CondTab(indx).reclassisn=AgrFacultType THEN
              IF SectRec.ReinsuranceAgrEarnedAmount=0 THEN
                vXLShare := 0;
                vXLShare := 0;
              ELSE
                SELECT --+ index (a X_RNP_RE_MSFO_XL_SECTION_ASC )
                       NVL(SUM(a.buhamountusd*a.coverageprem)/SectRec.ReinsuranceAgrEarnedAmount,0)
                INTO vXLShare
                FROM rnp_re_msfo_xl_section a
                WHERE a.loadisn=pLoadIsn
                 AND a.agrisn=CondTab(indx).agrisn
                 AND a.sectisn=CondTab(indx).sectisn
                 AND a.condisn=CondTab(indx).condisn
                 --{EGAO 15.07.2013 в рамках 50384368403
                 AND a.virtualdatebeg=CondTab(indx).virtualdatebeg
                 AND a.virtualdateend=CondTab(indx).virtualdateend
                 --}EGAO 15.07.2013
                 ;
              END IF;
              SectRec.RNPShare := CASE
                                    WHEN SectRec.Duration=0 THEN 0
                                    ELSE SectRec.Depospremsum*SectRec.Remainder/SectRec.Duration
                                  END * vXLShare;
            --}
            ELSIF SectRec.EmptyRateLayerCnt=0 THEN
              OPEN layer_cur(CondTab(indx).sectisn);
              LOOP
                FETCH layer_cur INTO layer_rec;
                EXIT WHEN layer_cur%NOTFOUND;
                vLayerShare := vLayerShare + layer_rec.rate*layer_rec.sharepc;
              END LOOP;
              SectRec.RNPShare := vLayerShare*CondTab(indx).shareneorig*CoverageRec.Rnp;
            END IF;
            SectRec.RNPShare_t := SectRec.RNPShare;
          ELSIF CondTab(indx).secttype='RX' THEN
            IF (CondTab(indx).reclassisn=AgrObligType AND CondTab(indx).redatebase='I') OR
               (CondTab(indx).reclassisn=AgrObligType AND CondTab(indx).redatebase='C' AND SectRec.EmptyDeposPremLayerCnt=0 AND SectRec.EmptyRateLayerCnt=0) THEN
              OPEN cond2sect_cur(CondTab(indx).agrisn, CondTab(indx).condisn, CondTab(indx).secttype);
              FETCH cond2sect_cur INTO cond2sect_rec;

              OPEN layer_cur(CondTab(indx).sectisn);
              LOOP
                FETCH layer_cur INTO layer_rec;
                EXIT WHEN layer_cur%NOTFOUND;

                IF cond2sect_rec.SectType='QS' THEN
                    vLayerShare := CASE
                                    WHEN CondTab(indx).sisum=0 OR cond2sect_rec.sharepc>=1 THEN 0
                                    ELSE least(greatest((CondTab(indx).sisum*(1-cond2sect_rec.sharepc)-layer_rec.prioritysum)/(CondTab(indx).sisum*(1-cond2sect_rec.sharepc)),0
                                                       ),layer_rec.limitsum/(CondTab(indx).sisum*(1-cond2sect_rec.sharepc)))
                                  END;
                ELSIF cond2sect_rec.SectType='XL' THEN
                  vPrioritySum := CASE
                                    WHEN cond2sect_rec.currisn=CondTab(indx).sectcurrisn THEN cond2sect_rec.prioritysum
                                    ELSE gcc2.gcc2(cond2sect_rec.prioritysum,cond2sect_rec.currisn,CondTab(indx).sectcurrisn,cond2sect_rec.datebeg)
                                  END;
                  vLayerShare := CASE
                                  WHEN vPrioritySum=0 THEN 0
                                  ELSE least(greatest((vPrioritySum-layer_rec.PrioritySum)/vPrioritySum,0
                                                     ), layer_rec.limitsum/vPrioritySum)
                                END;
                ELSE
                  vLayerShare := CASE
                                  WHEN CondTab(indx).sisum=0 THEN 0
                                  ELSE least(greatest((CondTab(indx).sisum-layer_rec.prioritysum)/CondTab(indx).sisum,0
                                                     ), layer_rec.limitsum/CondTab(indx).sisum)
                                END;
                END IF;
                SectRec.RNPShare := SectRec.RNPShare + layer_rec.rate*vLayerShare*layer_rec.sharepc;
                SectRec.RNPShare_t := SectRec.RNPShare_t + layer_rec.rate*vLayerShare;
              END LOOP;
              SectRec.RNPShare := SectRec.RNPShare*CondTab(indx).shareneorig*CoverageRec.Rnp;
              SectRec.RNPShare_t := SectRec.RNPShare_t*CondTab(indx).shareneorig*CoverageRec.Rnp;
            END IF;
          END IF;
        END IF;

        PrevSectElement.SharePc := SectRec.SharePc;
        IF CondTab(indx).secttype IN ('XL','SL') AND SectRec.EmptyRateLayerCnt>0 AND CondTab(indx).redatebase='C' AND CondTab(indx).reclassisn=AgrFacultType THEN
          PrevSectElement.RNPValue :=SectRec.RNPShare;
        ELSE
          PrevSectElement.RNPValue := (CondTab(indx).condbruttornp-SectRec.RNPPrevAmount)*SectRec.RNPShare_t*CondTab(indx).gn;
        END IF;

        IF PrevSectTab.exists(CondTab(indx).sectpriority) THEN
          IF CondTab(indx).secttype<>'RX' THEN
            PrevSectTab(CondTab(indx).sectpriority).rnpvalue := PrevSectTab(CondTab(indx).sectpriority).rnpvalue + PrevSectElement.RNPvalue;
          ELSIF CondTab(indx).secttype='RX' AND PrevSectTab(CondTab(indx).sectpriority).SharePc<PrevSectElement.SharePc THEN
            PrevSectTab(CondTab(indx).sectpriority) := PrevSectElement;
          END IF;

        ELSE
          PrevSectTab(CondTab(indx).sectpriority) := PrevSectElement;
        END IF;

        SectTab(to_char(CondTab(indx).sectisn)):= SectRec;
        vAgrIsn := CondTab(indx).agrisn;
        vCondIsn := CondTab(indx).condisn;
        vVirtualDateBeg := CondTab(indx).virtualdatebeg;
        vVirtualDateEnd := CondTab(indx).virtualdateend;
        vSectIsn := CondTab(indx).sectisn;
      END IF;

      IF CondTab(indx).secttype IN ('XL','SL') AND SectTab(to_char(CondTab(indx).sectisn)).EmptyRateLayerCnt>0 AND CondTab(indx).redatebase='C' AND CondTab(indx).reclassisn=AgrFacultType THEN
        vReRNP := SectTab(to_char(CondTab(indx).sectisn)).RNPShare;
        vReRNP_t := SectTab(to_char(CondTab(indx).sectisn)).RNPShare_t;
      ELSE
        IF CondTab(indx).bruttornp<0 THEN
          vReRNP := CondTab(indx).bruttornp*SectTab(to_char(CondTab(indx).sectisn)).RNPShare*CondTab(indx).gn;
          vReRNP_t := CondTab(indx).bruttornp*SectTab(to_char(CondTab(indx).sectisn)).RNPShare_t*CondTab(indx).gn;
        ELSE
          vReRNP := (CondTab(indx).bruttornp-SectTab(to_char(CondTab(indx).sectisn)).RNPPrevAmount)*SectTab(to_char(CondTab(indx).sectisn)).RNPShare*CondTab(indx).gn;
          vReRNP_t := (CondTab(indx).bruttornp-SectTab(to_char(CondTab(indx).sectisn)).RNPPrevAmount)*SectTab(to_char(CondTab(indx).sectisn)).RNPShare_t*CondTab(indx).gn;
        END IF;
      END IF;

      j := ResultTab.count+1;
      SELECT seq_rnp_re_msfo.nextval INTO vIsn FROM dual;

      ResultTab(j).isn:=vIsn;
      ResultTab(j).loadisn := pLoadIsn;
      ResultTab(j).daterep := vDateRep;
      -- соответствие прямог и исх. договоров
      ResultTab(j).agrxisn := CondTab(indx).agrxisn;
      ResultTab(j).shareneorig := CondTab(indx).shareneorig;
      -- исх. договор
      ResultTab(j).reisn := CondTab(indx).reisn;
      ResultTab(j).reid := CondTab(indx).reid;
      ResultTab(j).redatebase := CondTab(indx).redatebase;
      ResultTab(j).reclassisn := CondTab(indx).reclassisn;
      ResultTab(j).redatebeg := CondTab(indx).redatebeg;
      ResultTab(j).redateend := CondTab(indx).redateend;
      -- секция
      ResultTab(j).sectisn := CondTab(indx).sectisn;
      ResultTab(j).sectfullname := CondTab(indx).sectfullname;
      ResultTab(j).secttype := CondTab(indx).secttype;
      ResultTab(j).sectdatebeg := CondTab(indx).sectdatebeg;
      ResultTab(j).sectdateend := CondTab(indx).sectdateend;
      ResultTab(j).sectcurrisn := CondTab(indx).sectcurrisn;
      ResultTab(j).sectobjisn := CondTab(indx).sectobjisn;
      ResultTab(j).sectriskisn := CondTab(indx).sectriskisn;
      ResultTab(j).sectcommission := CondTab(indx).sectcommission;
      ResultTab(j).sectpriority := CondTab(indx).sectpriority;
      -- прямой договор
      ResultTab(j).condisn := CondTab(indx).condisn;
      ResultTab(j).agrisn := CondTab(indx).agrisn;
      ResultTab(j).agrdatebeg := CondTab(indx).agrdatebeg;
      ResultTab(j).agrdateend := CondTab(indx).agrdateend;
      ResultTab(j).agrcurrisn := CondTab(indx).agrcurrisn;
      ResultTab(j).agrclassisn := CondTab(indx).agrclassisn;
      ResultTab(j).buhdeptisn := CondTab(indx).buhdeptisn;
      -- условие прямого договора
      ResultTab(j).datebeg := CondTab(indx).datebeg;
      ResultTab(j).dateend := CondTab(indx).dateend;
      ResultTab(j).virtualdatebeg := CondTab(indx).virtualdatebeg;
      ResultTab(j).virtualdateend := CondTab(indx).virtualdateend;
      ResultTab(j).lengthtype := CondTab(indx).lengthtype;
      ResultTab(j).gn := CondTab(indx).gn;
      ResultTab(j).sisum := CondTab(indx).sisum;
      ResultTab(j).sicurrdate := CondTab(indx).sicurrdate;
      ResultTab(j).bruttornp := CondTab(indx).bruttornp;
      ResultTab(j).condbruttornp := CondTab(indx).condbruttornp;
      -- рассчитанные показатели
      ResultTab(j).rernp := vReRNP;
      ResultTab(j).rernpprev := SectTab(to_char(CondTab(indx).sectisn)).RNPPrevAmount;
      ResultTab(j).coveragesharepc := CoverageRec.Rnp;
      -- вспомогательные поля
      ResultTab(j).sectduration := SectTab(to_char(CondTab(indx).sectisn)).Duration;
      ResultTab(j).sectremainder:= SectTab(to_char(CondTab(indx).sectisn)).Remainder;
      ResultTab(j).sectprorata := SectTab(to_char(CondTab(indx).sectisn)).Remainder/SectTab(to_char(CondTab(indx).sectisn)).Duration;
      ResultTab(j).sharernp := SectTab(to_char(CondTab(indx).sectisn)).RNPShare;
      ResultTab(j).sharernpprev := SectTab(to_char(CondTab(indx).sectisn)).RNPShare_t;
      ResultTab(j).sectnachprem := SectTab(to_char(CondTab(indx).sectisn)).NachPrem;
      ResultTab(j).sectepi := SectTab(to_char(CondTab(indx).sectisn)).vEPI;
      ResultTab(j).sectemptyratelayercnt := SectTab(to_char(CondTab(indx).sectisn)).EmptyRateLayerCnt;
      ResultTab(j).sectdepospremsum := SectTab(to_char(CondTab(indx).sectisn)).DepospremSum;
      ResultTab(j).sectreinsuranceagrbuhamount := SectTab(to_char(CondTab(indx).sectisn)).ReinsuranceAgrBuhAmount;
      ResultTab(j).sectlessthanayearagrcnt := SectTab(to_char(CondTab(indx).sectisn)).LessThanAYearAgcCnt;
      ResultTab(j).SectEmptyDeposPremLayerCnt := SectTab(to_char(CondTab(indx).sectisn)).EmptyDeposPremLayerCnt;
      ResultTab(j).rptgroupisn := CondTab(indx).rptgroupisn;
      ResultTab(j).sagroup := CondTab(indx).sagroup;
      ResultTab(j).isrevaluation:=CondTab(indx).isrevaluation;
      ResultTab(j).rptclass:=CondTab(indx).rptclass;
      ResultTab(j).LongCondPremPc:= vLongCondPremPc;
      ResultTab(j).bruttornpagr:= CondTab(indx).bruttornpagr;

      IF MOD(j,1000) = 0 THEN
        FORALL indx IN ResultTab.FIRST .. ResultTab.LAST
        INSERT INTO rnp_re_msfo_rnp
        VALUES ResultTab(indx);
        ResultTab.delete;
      END IF;

      IF layer_cur%ISOPEN THEN CLOSE layer_cur; END IF;
      IF cond2sect_cur%ISOPEN THEN CLOSE cond2sect_cur; END IF;
    END LOOP;

    FORALL indx IN ResultTab.FIRST .. ResultTab.LAST
    INSERT INTO rnp_re_msfo_rnp
    VALUES ResultTab(indx);

    COMMIT;
  END;

  PROCEDURE make_rnp_re_msfo_final(pLoadIsn IN NUMBER)
  IS
    vDateRep DATE := GetDateRep(pLoadIsn);

    vMinIsn number:=-9999;
    vMaxIsn number;
    vSql varchar2(4000);
    SesId Number;
    vLoadObjCnt number:=10000;
    vCnt number:=0;
    vPart VARCHAR2(30);

  BEGIN
    IF vDateRep IS NULL THEN
      raise_application_error(en_invalid_daterep,'Invalid daterep');
    END IF;

    vPart:=init_partition_by_key(pTableName => 'storages.rnp_re_msfo_final',pKey => pLoadIsn,pCompress => 1);
    dbms_lock.sleep(20);
    SesId:=Parallel_Tasks.createnewsession();
    LOOP
      vMaxIsn := Cut_Table('storages.rep_agrre','sectisn',vMinIsn,pRowCount=>vLoadObjCnt);
      EXIT WHEN vMaxIsn IS NULL;

      vSql:=' declare
                vMinIsn number :='||vMinIsn||';
                vMaxIsn number :='||vMaxIsn||';
                vCnt    number :='||vCnt||';
                vLoadIsn number := '||pLoadIsn||';
              Begin
                DBMS_APPLICATION_INFO.SET_MODULE(''rnp_re_msfo_final'',''Precess#''||vCNT);
                storages.report_rnp_new.make_rnp_re_msfo_final_by_sect(vLoadIsn, vMinIsn, vMaxIsn);
              End;';

      System.Parallel_Tasks.processtask(sesid,vsql);

      vCnt:=vCnt+1;

      vMinIsn:=vMaxIsn;
      DBMS_APPLICATION_INFO.set_module('rnp_re_msfo','Applied: '||vCnt*vLoadObjCnt);

    END LOOP;
    -- ждем, пока завершатся все джобы
    Parallel_Tasks.endsession(sesid);
  END;
  
  PROCEDURE make_rnp_re_msfo_final_by_sect(pLoadIsn IN NUMBER, pMinIsn IN NUMBER, pMaxIsn IN NUMBER)
  IS
    vDateRep DATE := GetDateRep(pLoadIsn);
  BEGIN
    IF vDateRep IS NULL THEN
      raise_application_error(en_invalid_daterep,'Invalid daterep');
    END IF;

    pparam.Clear;
    pparam.SetParamN('MinSectIsn', pMinIsn);
    pparam.SetParamN('MaxSectIsn', pMaxIsn);
    pparam.SetParamN('LoadIsn', pLoadIsn);

    INSERT INTO storages.rnp_re_msfo_final(
     isn, loadisn, daterep, reclassisn, reid, reisn, redatebase,
     sectisn, secttype, sectfullname, sectdatebeg, sectdateend,
     sectcurrisn, agrid, agrisn, agrclassisn, dcioisn, sectobjisn,
     sectriskisn, buhdeptisn, sectcommission, sectnachprem, rernp,
     reprem, deferredincome, repremcharge, recommissioncharge,
     condisn, ssource, agrdatebeg, agrdateend, datebeg, dateend,
     agrruleisn, agrcomission, agrcurrisn, rptgroupisn

    )
    SELECT storages.seq_rnp_re_msfo.nextval,
           a.loadisn, a.daterep, a.reclassisn, a.reid,
           a.reisn, a.redatebase, a.sectisn, a.secttype, a.sectfullname,
           a.sectdatebeg, a.sectdateend, a.sectcurrisn, a.agrid, a.agrisn,
           a.agrclassisn, a.dcioisn, a.sectobjisn, a.sectriskisn,
           a.buhdeptisn, a.sectcommission, a.sectnachprem, a.rernp,
           a.reprem, a.deferredincome, a.repremcharge, a.recommissioncharge,
           a.condisn, a.ssource, a.agrdatebeg, a.agrdateend, a.datebeg,
           a.dateend, a.agrruleisn, a.agrcomission, a.agrcurrisn, a.rptgroupisn
    FROM storages.v_rnp_re_msfo_final a;

    COMMIT;
  END;  

  PROCEDURE make_rnp_re_subject(pLoadIsn IN NUMBER := NULL)
  IS
     vLoadIsn NUMBER := nvl(pLoadIsn, GetActiveLoad(trunc(SYSDATE,'mm')-1));
     vDateRep DATE := GetDateRep(vLoadIsn);
     vPart VARCHAR2(30);
     vMinIsn Number:=-1;
     vMaxIsn Number := 0;
     vCnt Number:=0;
     sesid NUMBER;
     vSql Varchar2(4000);
     LoadObjCnt  constant Number := 50000;
  BEGIN
    IF vDateRep IS NULL THEN
      raise_application_error(en_invalid_daterep,'Invalid daterep');
    END IF;
    /*pparam.SetParamN('LoadIsn',vLoadIsn);
    
    vPart:=init_partition_by_key(pTableName => 'storages.rnp_re_msfo_subjbysect',pKey => vLoadIsn,pCompress => 1);
    vPart:=init_partition_by_key(pTableName => 'storages.rnp_re_msfo_subjbyagrrole',pKey => vLoadIsn,pCompress => 1);
    vPart:=init_partition_by_key(pTableName => 'storages.rnp_re_rsbu_subjbysect',pKey => vLoadIsn,pCompress => 1);
    vPart:=init_partition_by_key(pTableName => 'storages.rnp_re_rsbu_subjbyagrrole',pKey => vLoadIsn,pCompress => 1);
    
    
    INSERT INTO rnp_re_msfo_subjbysect(
      isn,
      loadisn,daterep,
      sectisn,sectdatebeg,sectdateend,secttype,
      reisn,reinsisn,reinsisnname,reinsisnpc,shortname,fullname,latname,country,
      ingo,reinsdateentry,reinsdateswitch,reinsdatebeg,reinsdateend,recode,redatebase, ssource
    )
    SELECT 
      storages.seq_rnp_re_msfo.nextval,
      loadisn,daterep,
      sectisn,sectdatebeg,sectdateend,secttype,
      reisn,reinsisn,reinsisnname,reinsisnpc,shortname,fullname,latname,country,
      ingo,reinsdateentry,reinsdateswitch,reinsdatebeg,reinsdateend,recode,redatebase, ssource
    FROM storages.v_rnp_re_msfo_subjbysect a;
    
    COMMIT;
    
    INSERT INTO rnp_re_msfo_subjbyagrrole(
      isn, 
      ssource,loadisn,daterep,agrisn,reinsisn,reinsisnname,reinsisnpc,
      shortname,fullname,latname,country,ingo,recode
    )
    SELECT storages.seq_rnp_re_msfo.nextval,
           a.ssource, a.loadisn, a.daterep, a.agrisn, a.reinsisn, a.reinsisnname, a.reinsisnpc,
           a.shortname, a.fullname, a.latname, a.country, a.ingo, a.recode
    FROM v_rnp_re_msfo_subjbyagrrole a;
    
    COMMIT;

    
  
    INSERT INTO rnp_re_rsbu_subjbyagrrole(
      isn,
      ssource,loadisn,daterep,agrisn,reinsisn,reinsisnname,reinsisnpc,
      shortname,fullname,latname,country,ingo,recode
    )
    SELECT storages.seq_rnp_re_msfo.nextval,
           a.ssource, a.loadisn, a.daterep, a.agrisn, a.reinsisn, a.reinsisnname, a.reinsisnpc,
           a.shortname, a.fullname, a.latname, a.country, a.ingo, a.recode
    FROM v_rnp_re_rsbu_subjbyagrrole a;
      
    COMMIT;
    
    SesId:=Parallel_Tasks.createnewsession();
    vMinIsn := -1; -- !!! В rnp_re_rsbu есть записи с condisn=0
    LOOP
      SELECT max (condisn)
      INTO  vMaxIsn
      FROM (
            SELECT --+ index (b X_RNP_RE_RSBU_CONDISN)
                   condisn
            FROM storages.rnp_re_rsbu b
            WHERE b.condisn > vMinIsn AND b.loadisn=vLoadIsn
              AND ROWNUM <= LoadObjCnt
           );

      IF (vMaxIsn IS NULL) THEN EXIT; END IF;

      vSql:=' Declare
                vLoadIsn    number :='||vLoadIsn||';
                vMinIsn     number := '||vMinIsn||';
                vMaxIsn     number := '||vMaxIsn||';
                vCnt        number :='||vCnt||';
                vDateRep Date := report_rnp_new.GetDateRep(vLoadIsn);
              Begin
                dbms_application_info.Set_Module(''rnp_re_rsbu_subjbysect'',''Thread: ''||vCnt);
                --storages.report_rnp_new.make_rnp_msfo_r_by_isn(vLoadIsn, vMinIsn, vMaxIsn);
                
                pparam.setparamn(''CondMinIsn'',vMinIsn);
                pparam.setparamn(''CondMaxIsn'',vMaxIsn);              
                pparam.setparamn(''LoadIsn'',vLoadIsn);              
                
                
                INSERT INTO storages.rnp_re_rsbu_subjbysect(
                  isn,loadisn,daterep,agrisn,agrdatebeg,agrdateend,condisn,
                  conddatebeg,conddateend,sectisn,sectdatebeg,sectdateend,secttype,
                  reisn,reinsisn,reinsisnname,reinsisnpc,shortname,fullname,latname,country,
                  ingo,reinsdateentry,reinsdateswitch,reinsdatebeg,reinsdateend,recode,redatebase, ssource
                  

                )
                SELECT storages.seq_rnp_re_msfo.nextval,
                       loadisn, daterep, agrisn,agrdatebeg,agrdateend,condisn,
                  conddatebeg,conddateend,sectisn,sectdatebeg,sectdateend,secttype,
                  reisn,reinsisn,reinsisnname,reinsisnpc,shortname,fullname,latname,country,
                  ingo,reinsdateentry,reinsdateswitch,reinsdatebeg,reinsdateend,recode,redatebase, ssource
                FROM storages.v_rnp_re_rsbu_subjbysect a; 
                
                commit;
              end;';
      Parallel_Tasks.processtask(sesid,vsql);
      vMinIsn:=vMaxIsn;
      vCnt:=vCnt+1;
      dbms_application_info.Set_Module('rnp_re_rsbu_subjbysect','Applied:'||vCnt*LoadObjCnt);
    END LOOP;

    Parallel_Tasks.endsession(sesid);*/
    
    pparam.SetParamN('LoadIsn',vLoadIsn);
    vPart:=init_partition_by_key(pTableName => 'storages.rnp_re_subject',pKey => vLoadIsn,pCompress => 1);
    dbms_lock.sleep(10);

    DBMS_APPLICATION_INFO.SET_MODULE('rnp_re_msfo','rnp_re_subject_by_section');
    INSERT INTO storages.rnp_re_subject(
      isn, loadisn, daterep, reschema, reagrisn, sectisn,
      reinsisn, reinsisnname, reinsisnpc, shortname, fullname,
      latname, country, ingo, recode,
      reinsdateentry, reinsdateswitch, reinsdatebeg, reinsdateend, sectdatebeg, sectdateend

    )
    SELECT storages.seq_rnp_re_msfo.nextval,
           a.loadisn, a.daterep, ReSchemaBySection, a.reisn, a.sectisn,
           a.reinsisn, a.reinsisnname, a.reinsisnpc, a.shortname, a.fullname,
           a.latname, a.country, ingo, a.recode, a.reinsdateentry, 
           a.reinsdateswitch, a.reinsdatebeg, a.reinsdateend, a.sectdatebeg, a.sectdateend
    FROM storages.v_rnp_re_subject_by_section a;

    DBMS_APPLICATION_INFO.SET_MODULE('rnp_re_msfo','rnp_re_subject_by_agrrole');

    INSERT INTO storages.rnp_re_subject(
      isn, loadisn, daterep, reschema, agrisn, reinsisn,
      reinsisnname, reinsisnpc, shortname, fullname,
      latname, country, ingo, recode
    )
    SELECT storages.seq_rnp_re_msfo.nextval,
           a.loadisn, a.daterep, ReSchemaByRole, a.agrisn, a.reinsisn,
           a.reinsisnname, a.reinsisnpc, a.shortname, a.fullname,
           a.latname, a.country, a.ingo, a.recode
    FROM storages.v_rnp_re_subject_by_agrrole a;

    COMMIT;

  END;
  
  -- в рамках ДИТ-10-3-120935
  PROCEDURE make_rnposagoshortagr(pLoadIsn IN NUMBER)
  IS
    vDateRep DATE := GetDateRep(pLoadIsn);
    vMinIsn number:=-9999;
    vMaxIsn number;
    vSql varchar2(4000);
    SesId Number;
    vLoadObjCnt number:=100000;
    vCnt number:=0;
    vPart VARCHAR2(30);
  BEGIN
    IF vDateRep IS NULL THEN
      raise_application_error(en_invalid_daterep,'Invalid daterep');
    END IF;  
    
    vPart:=init_partition_by_key(pTableName => 'storages.rnposagoshortagr',pKey => pLoadIsn,pCompress => 1);
    SesId:=Parallel_Tasks.createnewsession;

    LOOP
      vMaxIsn:=Cut_Table('storage_source.repagr','agrisn',vMinIsn,pRowCount=>vLoadObjCnt);
      EXIT WHEN vMaxIsn IS NULL;
      vSql:= 'DECLARE
                vLoadIsn number :='||pLoadIsn||';
                vMinIsn number :='||vMinIsn||';
                vMaxIsn number :='||vMaxIsn||';
                vCnt    number :='||vCnt||';
              BEGIN
                DBMS_APPLICATION_INFO.SET_MODULE(''rnposagoshortagr'',''Precess#''||vCNT);

                insert into storages.rnposagoshortagr(loadisn, agrisn, rnpshare)
                SELECT  vLoadIsn, 
                        agrisn, 
                        CASE WHEN max(AllPeriodLength)=0 THEN 0 ELSE  least(1, max(RnpPeriodLength)/max(AllPeriodLength)) END AS rnpshare
                FROM (
                      SELECT --+ index ( ra X_REPAGR_AGR ) ordered use_nl ( ra cd ) no_merge ( dt )
                             ra.agrisn, trunc(ra.datebeg) AS adatebeg, trunc(ra.dateend) AS adateend,
                             (trunc(ra.dateend)-trunc(ra.datebeg)+1) AS agrlength,
                             trunc(cd.datebeg) AS conddatebeg,
                             trunc(cd.dateend) AS conddateend,
                             SUM(trunc(cd.dateend)-trunc(cd.datebeg)+1) over (PARTITION BY cd.agrisn) AS AllPeriodLength,
                             SUM(CASE
                                   WHEN cd.datebeg>dt.daterep THEN cd.dateend-cd.datebeg+1
                                   WHEN cd.dateend>dt.daterep THEN cd.dateend-dt.daterep
                                   ELSE 0
                                 END) over (PARTITION BY cd.agrisn) AS RnpPeriodLength
                      FROM repagr ra, repcond cd,
                           (SELECT a.daterep FROM repload a where a.isn=vLoadIsn) dt
                      WHERE ra.agrisn > vMinIsn And ra.agrisn <= vMaxIsn
                        AND ra.ruleisn=753518300
                        AND cd.agrisn=ra.agrisn
                        AND cd.newaddisn IS NULL
                     ) a
                WHERE agrlength>AllPeriodLength
                GROUP BY agrisn;

               COMMIT;
             END;';

      System.Parallel_Tasks.processtask(sesid,vsql);

      vCnt:=vCnt+1;

      vMinIsn:=vMaxIsn;
      DBMS_APPLICATION_INFO.set_module('rnposagoshortagr','Applied: '||vCnt*vLoadObjCnt);

    END LOOP;

    -- ждем, пока завершатся все джобы
    Parallel_Tasks.endsession(sesid);
    
    
  END;


END;
