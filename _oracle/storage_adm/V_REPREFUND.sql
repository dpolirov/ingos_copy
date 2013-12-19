CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_REPREFUND" ("REFUNDISN", "AGRISN", "CONDISN", "CURRISN", "CLAIMSUM", "DATELOSS", "DATECLAIM", "SUBJISN", "STATUS", "DATESOLUTION", "CLAIMSTATUS", "DATEEVENT", "DEPTISN", "FRANCHTYPE", "FRANCHTARIFF", "FRANCHSUM", "AGRDATEBEG", "RPTCLASSISN", "LOSSSHARE", "CLAIMISN", "DATEREG", "EMPLISN", "OBJISN", "PARENTOBJISN", "RPTGROUPISN", "CONDDEPTISN", "ISREVALUATION", "FRANCHCURRISN", "FRANCHDEDUCTED", "CLASSISN", "REFUNDSUM", "REFUNDSUMUSD", "CLAIMSUMUSD", "CLAIMID", "FIRMISN", "DATEREFUND", "LIMITSUM", "LIMITCURRISN", "RULEISNAGR", "RULEISNCLAIM", "NRZU", "BUDGETGROUPISN", "OBJCLASSISN", "AGREXTISN", "CONDPC", "PARENTOBJCLASSISN", "RAGRISN", "EXTDATEEVENT", "TOTALLOSS", "RFRANCHCURRISN", "RFRANCHSUM", "SALEREMPLISN", "SALERDEPTISN", "MOTIVGROUPISN", "RISKRULEISN", "RISKCLASSISN", "RDATEVAL", "REPDATELOSS", "CLAIMDATETOTALLOSS", "CLAIMCURRISN", "REFUNDSUMRUB", "CLAIMSUMRUB", "REGRESS", "CLAIMCLASSISN", "REFCREATED", "PARENTISN", "AGGRIEVEDNUMBER", "REFUNDID", "AGRCLASSISN") AS 
  (/* KGS 10.01.2013    REPCOND нельзя использовать - в нем теперь нет медиц кондов с 0 плановой премией*/
select --+ use_nl(s ac ar ao ) ordered push_subq index ( rc x_repcond_cond )
       S.ISN REFUNDISN,
       S.AGRISN,
       S.CONDISN,
       S.CURRISN,
       S.CLAIMSUM,
       S.DATELOSS,
       S.DATECLAIM,
       S.SUBJISN,
       S.REFSTATUS STATUS,
       S.DATESOLUTION,
       S.CLSTATUS CLAIMSTATUS,
       S.DATEEVENT,
       S.DEPTISN,
       AC.FRANCHTYPE,
       AC.FRANCHTARIFF,
       AC.FRANCHSUM,
       S.AGRDATEBEG,
       S.RPTCLASSISN,
       LOSSSHARE,
       S.CLAIMISN,
       S.DATEREG,
       S.EMPLISN,
       S.OBJISN,
       CASE WHEN AO.PARENTISN IS NULL THEN AO.ISN
        ELSE
         (  select --+ rule
             Max(ZZ.ISN)
             from AGROBJECT ZZ
            where ZZ.PARENTISN is null
            start with ZZ.ISN = AO.PARENTISN
             connect by NoCYCLE prior ZZ.PARENTISN = ZZ.ISN
          )  END  PARENTOBJISN,
--!!       Nvl(RC.PARENTOBJISN,RC.OBJISN) PARENTOBJISN,
       to_Number(null) RPTGROUPISN, --поле заполняется после загрузки reprefund с помощью апдейта
       to_Number(null) CONDDEPTISN,
       to_Number(null) ISREVALUATION, --поля заполняются после загрузки reprefund с помощью апдейт
       AC.FRANCHCURRISN,
       nvl(decode(nvl(AC.FRANCHTYPE, 'Б'), 'Б', decode (AC.FRANCHTARIFF, null, gcc2.gcc2(AC.FRANCHSUM, AC.FRANCHCURRISN, S.CURRISN, nvl(nvl(DATELOSS, DATEEVENT), DATEREG)))), 0) +
       nvl(S.CLAIMSUM * decode(nvl(AC.FRANCHTYPE, 'Б'), 'Б', AC.FRANCHTARIFF), 0) / 100 FRANCHDEDUCTED,
       S.CLASSISN,
       S.REFUNDSUM,
       S.REFUNDSUMUSD,
       S.CLAIMSUMUSD,
       S.ID CLAIMID,
       s.FIRMISN,
       S.DATEREFUND,
       AC.LIMITSUM,
       AC.CURRISN LIMITCURRISN,
       s.RULEISNAGR,
       S.RULEISN RULEISNCLAIM,
       S.NRZU,
       to_Number(null) BUDGETGROUPISN, --поле заполняется после загрузки reprefund с помощью апдейта
       AO.CLASSISN OBJCLASSISN,
       AGREXTISN,
       --MSerp 04.02.2011 ДИТ-11-1-128014 {
       --decode(ALLREFUNDSUM, 0, 1 / ALLREFUND, decode(nvl(REFUNDSUM, 0), 0, nvl(CLAIMSUM, 0), nvl(REFUNDSUM, 0)) / ALLREFUNDSUM) CONDPC,
       decode(ALLREFUNDSUM, 0, 1 / ALLREFUND, decode(nvl(S.REFUNDSUM, 0), 0, decode(AGREXTISN, null,nvl(S.CLAIMSUM, 0),0)  , nvl(S.REFUNDSUM, 0)) / ALLREFUNDSUM) CONDPC,
       --}ДИТ-11-1-128014

       CASE WHEN AO.PARENTISN IS NULL THEN NULL
        ELSE
         (  select --+ rule
             Max(ZZ.CLASSISN)
             from AGROBJECT ZZ
            where ZZ.PARENTISN is null
            start with ZZ.ISN = AO.PARENTISN
             connect by NoCYCLE prior ZZ.PARENTISN = ZZ.ISN
          )  END   PARENTOBJCLASSISN,
       RAGRISN,
       EXTDATEEVENT,
       TOTALLOSS,
       S.FRANCHCURRISN RFRANCHCURRISN,
       S.FRANCHSUM RFRANCHSUM,
       ( select max(SUBJISN)
           from AGRROLE AR
          where AR.AGRISN    = S.AGRISN
            and AR.REFUNDISN = S.ISN
            and AR.CLASSISN  = 1521585603 ) SALEREMPLISN,
       ( select max(DEPTISN)
           from AGRROLE AR
          where AR.AGRISN    = S.AGRISN
            and AR.REFUNDISN = S.ISN
            and AR.CLASSISN  = 1521585603 ) SALERDEPTISN,
       to_Number(null) MOTIVGROUPISN, --поле заполняется после заргузки reprefund процедурой set_refund_motivgroupisn
       AR.RULEISN RISKRULEISN,
       Ar.CLASSISN RISKCLASSISN,
       DATEVAL RDATEVAL,
       trunc(decode(AGREXTISN, null, nvl(nvl(DATELOSS, DATECLAIM), DATEREG), nvl(nvl(EXTDATEEVENT, nvl(DATELOSS, DATECLAIM)), DATEREG))) REPDATELOSS,
       S.CLAIMDATETOTALLOSS,
       S.CLAIMCURRISN, -- egao 20.03.2009 ДИТ-09-1-086869
       S.REFUNDSUMRUB,
       S.CLAIMSUMRUB,
       S.REGRESS,
       S.CLAIMCLASSISN, -- egao 27.10.2010 ДИТ-10-4-121049
       S.CREATED REFCREATED,-- OD 01.07.2011
       S.PARENTISN,
       (SELECT COUNT(DISTINCT subjisn) FROM agrrole rl  WHERE rl.agrisn=s.agrisn  AND rl.refundisn=s.isn AND rl.classisn=971382125) AS aggrievednumber, -- EGAO 20.03.2013 ДИТ-12-4-176083
       s.refundid,  -- EGAO 20.03.2013 ДИТ-12-4-176083
       s.agrclassisn
  from ( select s.ISN,
                s.CLAIMISN,
                s.AGRISN,
                s.RPTCLASSISN,
                s.CONDISN,
                s.CURRISN,
                s.CLAIMSUM,
                s.DATELOSS,
                s.DATECLAIM,
                s.DATEREG,
                s.DATESOLUTION,
                nvl(s.EXTDATEEVENT, s.DATEEVENT) DATEEVENT,
                s.SUBJISN,
                s.REFSTATUS,
                s.CLSTATUS,
                s.DEPTISN,
                s.DATEREFUND,
                s.FRANCHSUM,
                s.FRANCHCURRISN,
                s.AGRDATEBEG,
                s.LOSSSHARE,
                s.EMPLISN,
                s.CLASSISN,
                s.REFUNDSUM,
                s.OBJISN,
                gcc2.gcc2(s.REFUNDSUM, s.CURRISN, 53, nvl(s.DATEREFUND, s.DATEEVENT)) REFUNDSUMUSD,
                gcc2.gcc2(s.CLAIMSUM, s.CURRISN, 53, nvl(s.DATELOSS, s.DATECLAIM)) CLAIMSUMUSD,
                gcc2.gcc2(s.REFUNDSUM, s.CURRISN, 35, nvl(s.DATEREFUND, s.DATEEVENT)) REFUNDSUMRUB,
                gcc2.gcc2(s.CLAIMSUM, s.CURRISN, 35, nvl(s.DATELOSS, s.DATECLAIM)) CLAIMSUMRUB,
                s.ID,
                s.RULEISN,
                s.NRZU,
                s.AGREXTISN,
                --MSerp 04.02.2011 ДИТ-11-1-128014 {
                --sum(decode(nvl(decode(EXT.ISN, null, R.REFUNDSUM, EXT.REFUNDSUM), 0), 0, nvl(decode(EXT.ISN, null, R.CLAIMSUM, EXT.CLAIMSUM), 0), decode(EXT.ISN, null, R.REFUNDSUM, EXT.REFUNDSUM)))over(partition by R.ISN) ALLREFUNDSUM,

                sum(decode(nvl(s.REFUNDSUM, 0), 0, nvl(decode(s.AGREXTISN, null, s.CLAIMSUM, s.REFUNDSUM), 0), s.REFUNDSUM))over(partition by s.ISN) ALLREFUNDSUM,
                --} ДИТ-11-1-128014
                count(*)over(partition by s.ISN) ALLREFUND,
                s.RAGRISN,
                s.EXTDATEEVENT,
                s.TOTALLOSS,
                s.DATEVAL,
                /*EGAO 21.11.2012
                  ( select min(Q.DATESEND)
                    from AIS.QUEUE Q
                   where Q.CLASSISN = 1647725903 -- c.get('qeclaimtotal')
                     and Q.OBJISN   = s.CLAIMISN )*/ to_date(NULL) AS CLAIMDATETOTALLOSS,
                S.CLAIMCURRISN,
                s.REGRESS,
                s.CLAIMCLASSISN, -- egao 27.10.2010 ДИТ-10-4-121049
                s.CREATED, -- OD 01.07.2011
                s.firmisn, s.ruleisnagr,
                s.PARENTISN,
                s.refundid,
                s.agrclassisn
           from (
                 SELECT --+ ordered use_nl ( s ag ) no_merge ( s ) index ( ag X_REPAGR_AGR )
                        ag.firmisn, ag.ruleisn AS ruleisnagr, ag.classisn AS agrclassisn,
                        s.*
                 FROM (
                       SELECT --+ ordered use_nl ( t cl r ext cr )
                              R.ISN,
                              R.CLAIMISN,
                              r.agrisn AS ragrisn,
                              nvl(EXT.AGRISN, R.AGRISN) AS AGRISN,
                              R.RPTCLASSISN,
                              nvl(EXT.CONDISN, R.CONDISN) CONDISN,
                              decode(EXT.ISN, null, R.CURRISN, EXT.CURRISN) CURRISN,
                              decode(EXT.ISN, null, R.CLAIMSUM, EXT.CLAIMSUM) CLAIMSUM,
                              CL.DATELOSS,
                              CL.DATECLAIM,
                              CL.DATEREG,
                              CL.DATESOLUTION,
                              EXT.DATEEVENT AS EXTDATEEVENT, R.DATEEVENT,
                              CL.SUBJISN,
                              R.STATUS REFSTATUS,
                              CL.STATUS CLSTATUS,
                              nvl(R.DEPTISN, CL.DEPTISN) DEPTISN,
                              R.DATEREFUND,
                              R.FRANCHSUM,
                              R.FRANCHCURRISN,
                              CL.AGRDATEBEG,
                              r.LOSSSHARE,
                              nvl(R.EMPLISN, CL.EMPLISN) EMPLISN,
                              nvl(EXT.CLASSISN, R.CLASSISN) CLASSISN,
                              decode(EXT.ISN, null, R.REFUNDSUM, EXT.REFUNDSUM) REFUNDSUM,
                              nvl(EXT.OBJISN, R.OBJISN) OBJISN,
                              CL.ID,
                              CL.RULEISN,
                              R.NRZU,
                              EXT.ISN AGREXTISN,
                              CL.CURRISN AS CLAIMCURRISN,
                              R.REGRESS,
                              CL.CLASSISN CLAIMCLASSISN, -- egao 27.10.2010 ДИТ-10-4-121049
                              R.CREATED, -- OD 01.07.2011
                              R.DATEVAL,
                              CR.TOTALLOSS,
                              R.PARENTISN,
                              r.refundid -- EGAO 20.03.2013 ДИТ-12-4-176083
                       FROM TT_ROWID T,
                            AIS.AGRCLAIM CL,
                            AIS.AGRREFUND R,
                            AIS.AGRREFUNDEXT EXT,
                            AIS.CLAIMREFUNDCAR CR
                       where T.ISN      = CL.ISN
                         and R.CLAIMISN = CL.ISN
                         and R.ISN      = EXT.REFUNDISN(+)
                         and R.ISN      = CR.ISN(+)
                         and R.EMPLISN not in ( select --+ index (sb x_subject_class)
                                                       ISN
                                                from SUBJECT SB
                                                where CLASSISN = 491 ) -- Тестовый пользователь
                         and nvl(CL.CLASSISN, 0) <> 2835056703 -- акция "помощь друга" od 27.11.2009 12475086503
                      ) s, repagr ag
                 WHERE S.AGRISN=AG.AGRISN(+)
                ) s
            WHERE nvl(s.agrclassisn,0)<>28470016
           ) S,
         AGRCOND AC,
         AGRRISK AR,
         AGROBJECT AO

 where S.CONDISN=Ac.Isn(+)
   and AC.RISKISN=AR.ISN(+)
   and AC.OBJISN=AO.ISN(+)
);