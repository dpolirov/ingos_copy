CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_REPAGR" ("AGRISN", "ID", "DATEBEG", "DATEEND", "DATESIGN", "CLASSISN", "RULEISN", "DEPTISN", "DEPT0ISN", "FILISN", "RULEDEPT", "EMPLISN", "CLIENTISN", "CURRISN", "PREMIUMSUM", "PREMUSD", "PREMRUB", "PREMEUR", "INCOMERATE", "STATUS", "DISCR", "APPLISN", "SHAREPC", "REINSPC", "GROUPISN", "BIZFLG", "PARENTISN", "INSURANTISN#", "INSURANTCOUNT#", "AGENTISN", "AGENTCOUNT", "EMITISN", "EMITCOUNT", "COMISSION", "BUHDATE", "LIMITSUM", "LIMITSUMUSD", "INSUREDSUM", "INSUREDSUMUSD", "AGRCREATED", "AGENTJURIDICAL", "FIRMISN", "AGENTCLASSISN", "SALERGOISN", "SALERFISN", "OWNERDEPTISN", "CLIENTJURIDICAL", "FILCOMMISION", "BEMITISN", "BFILISN", "CALCBIZFLG", "PREVISN", "CROSSALERISN", "TRANSFERCOMISSION", "BENEFICIARYISN", "PARTNERISN", "LIMITSUMRUB", "INSUREDSUMRUB", "OLDDATEEND", "CALCEMITISN", "CALCFILISN", "GMISN", "ADDRISN", "AGENTDEPTISN", "DATECALC", "BROKERISN", "LEASEFICIARYISN", "AGENTCOLLECTFLG", "AGRDETAILISN", "PAWNBROKERISN", "SALESCHANNELISN", "DATEBASE", "RECOMMENDERISN", "FORMISN", "CREATEDBY", "UPRISN", "INCOMESUM", "INCOMESUMUSD", "INCOMESUMRUB", "DISCOUNT", "DATEISSUE", "CREATEDATE") AS 
  (select --+ ordered use_hash(sd  fil bfil) Use_Nl(ar)
       S.AGRISN, S.ID, S.DATEBEG, S.DATEEND, S.DATESIGN, S.CLASSISN, S.RULEISN, S.DEPTISN, nvl(SD.DEPT0ISN, 0) DEPT0ISN,
       FIL.FILISN, S.RULEDEPT, S.EMPLISN, S.CLIENTISN, S.CURRISN, S.PREMIUMSUM, S.PREMUSD, S.PREMRUB, S.PREMEUR, S.INCOMERATE,
       S.STATUS, S.DISCR, S.APPLISN, S.SHAREPC, S.REINSPC, S.GROUPISN, S.BIZFLG, S.PARENTISN, S.INSURANTISN#, S.INSURANTCOUNT#,
       Nvl(AR.AGENTISN,AR.BROKERISN) AGENTISN/* KGR 10.05.2011*/, AR.AGENTCOUNT, AR.EMITISN, AR.EMITCOUNT, S.COMISSION, S.BUHDATE, S.LIMITSUM, S.LIMITSUMUSD, S.INSUREDSUM,
       S.INSUREDSUMUSD, S.AGRCREATED, NVL(AR.AGENTJURIDICAL,AR.BROKERJURIDICAL) AGENTJURIDICAL /* KGR 10.05.2011*/,
       S.FIRMISN, 437 AGENTCLASSISN, AR.SALERGOISN, AR.SALERFISN, S.OWNERDEPTISN,
       S.CLIENTJURIDICAL, AR.FILCOMMISION, AR.BEMITISN, BFIL.FILISN BFILISN,
       decode(decode(BEMITISN, null, FIL.RCISN, BFIL.RCISN), null, 'Ц', 'Ф') CALCBIZFLG,
       S.PREVISN, AR.CROSSALERISN, AR.TRANSFERCOMISSION, AR.BENEFICIARYISN, AR.PARTNERISN, S.LIMITSUMRUB, S.INSUREDSUMRUB,
       S.OLDDATEEND, nvl(BEMITISN, EMITISN) CALCEMITISN,
       decode(BEMITISN, null, FIL.FILISN, BFIL.FILISN) CALCFILISN,
       S.GMISN, S.ADDRISN, AR.AGENTDEPTISN, S.DATECALC, AR.BROKERISN, AR.LEASEFICIARYISN, AR.AGENTCOLLECTFLG, S.AGRDETAILISN,
       AR.PAWNBROKERISN, -- OD 6.05.2010 ДИТ-10-2-098743 Залогодержатель
       S.SALESCHANNELISN,
       S.DATEBASE,
       AR.RECOMMENDERISN, -- OD 07.09.2010 ДИТ-10-3-117604
       s.formisn, -- EGAO 04.03.2011
       S.CREATEDBY, -- OD 22.03.2011
       ar.uprisn, -- EGAO 18.04.2012
       -- sts 19.10.2012 - task(38397275003)
       S.INCOMESUM,
       S.INCOMESUMUSD,
       S.INCOMESUMRUB,
       -- sts 06.11.2012 - скидка для туристов
       S.DISCOUNT,
       s.dateissue, -- EGAO 08.05.2013
       s.CREATEDATE
  from ( select --+ ordered use_nl(r a ar dr drp ad sd sdf sdf1 arc clnt) use_hash(agnt gm)
                A.ISN AGRISN,A.ID, A.DATEBEG, A.DATEEND, A.DATESIGN, A.CLASSISN, A.RULEISN, A.DEPTISN,
                DR.FILTERISN RULEDEPT, A.EMPLISN, A.CLIENTISN, A.CURRISN, A.PREMIUMSUM,
                decode(A.CURRISN, 53, A.PREMIUMSUM, gcc2.gcc2(A.PREMIUMSUM, A.CURRISN, 53, least(A.DATESIGN, trunc(sysdate), A.DATEBEG))) PREMUSD,
                decode(A.CURRISN, 35, A.PREMIUMSUM, gcc2.gcc2(A.PREMIUMSUM, A.CURRISN, 35, least(A.DATESIGN, trunc(sysdate), A.DATEBEG))) PREMRUB,
                decode(A.CURRISN, 29448516, A.PREMIUMSUM, gcc2.gcc2(A.PREMIUMSUM, A.CURRISN, 29448516, least(A.DATESIGN, trunc(sysdate), A.DATEBEG))) PREMEUR,
                decode(A.PREMIUMSUM, 0, 0, A.INCOMESUM / A.PREMIUMSUM) INCOMERATE,
                A.STATUS, A.DISCR, A.APPLISN, A.SHAREPC, A.REINSPC, A.GROUPISN, A.BIZFLG, A.PARENTISN,
                /*INSURANTISN переименован в INSURANTISN# для того, чтобы выяснить где использовались эти поля.
                В дальнейшем, эти два поля надо из repagr убрать (вемсте с agrrole) */
                -- nvl(min(decode(AR.CLASSISN, 430, AR.SUBJISN)), CLIENTISN) INSURANTISN#,
                -- count(decode(AR.CLASSISN, 430, 1)) INSURANTCOUNT#,
                to_number(null) INSURANTISN#,
                to_number(null) INSURANTCOUNT#,
                A.COMISSION COMISSION,
                null BUHDATE,
                A.LIMITSUM,
                decode(A.CURRISN, 53, A.LIMITSUM, gcc2.gcc2(A.LIMITSUM, A.CURRISN, 53, least(A.DATESIGN, trunc(sysdate), A.DATEBEG))) LIMITSUMUSD,
                decode(A.CURRISN, 35, A.LIMITSUM, gcc2.gcc2(A.LIMITSUM, A.CURRISN, 35, least(A.DATESIGN, trunc(sysdate), A.DATEBEG))) LIMITSUMRUB,
                A.INSUREDSUM,
                decode(A.CURRISN, 53, A.INSUREDSUM, gcc2.gcc2(A.INSUREDSUM, A.CURRISN, 53, least(A.DATESIGN, trunc(sysdate), A.DATEBEG))) INSUREDSUMUSD,
                decode(A.CURRISN, 35, A.INSUREDSUM, gcc2.gcc2(A.INSUREDSUM, A.CURRISN, 35, least(A.DATESIGN, trunc(sysdate), A.DATEBEG))) INSUREDSUMRUB,
                A.CREATED AGRCREATED,
                A.FIRMISN,
                A.OWNERDEPTISN OWNERDEPTISN,
                CLNT.JURIDICAL CLIENTJURIDICAL,
                A.PREVISN PREVISN,
                A.OLDDATEEND OLDDATEEND,
                GM.GMISN GMISN,
                A.ADDRISN,
                A.DATECALC,
                AD.AGRDETAILISN AGRDETAILISN, -- OD 11.11.09 детализация дог-ра
                A.SALESCHANNELISN SALESCHANNELISN,
                a.datebase AS datebase, -- EGAO 05.07.2010
                a.formisn AS formisn, -- EGAO 0.03.2011
                A.CREATEDBY CREATEDBY, -- OD 22.03.2011
                -- sts 19.10.2012 - task(38397275003)
                A.INCOMESUM,
                system.gcc2.gcc2(A.INCOMESUM, A.CURRISN, 53, least(A.DATESIGN, trunc(sysdate), A.DATEBEG)) as INCOMESUMUSD,
                system.gcc2.gcc2(A.INCOMESUM, A.CURRISN, 35, least(A.DATESIGN, trunc(sysdate), A.DATEBEG)) as INCOMESUMRUB,
                -- sts 06.11.2012 - скидка для туристов
                A.DISCOUNT,
                a.dateissue, -- EGAO 08.05.2013
                null CREATEDATE
           from TT_ROWID R,
                AIS.AGREEMENT A,
                AIS.SUBJECT_T CLNT,
                -- AIS.AGRROLE AR,
                DICTI DR,
                AGR_DETAIL_AGRHASH AD, -- OD 11.11.09 Детализация договора
                ( select --+ ordered use_nl(x) index(x x_agrext_agr) use_hash(d)
                         AGRISN,
                         max(X1) GMISN
                    from TT_ROWID R,
                         AGREXT X,
                         ( select ISN
                             from DICTI Z
                            start with ISN = 2255842303 -- продукты gm
                          connect by prior ISN = PARENTISN ) D
                   where X.AGRISN   = R.ISN
                     and X.CLASSISN = 1071774425
                     and X.X1       = D.ISN
                   group by AGRISN ) GM
          where A.ISN          = R.ISN
            -- and A.ISN          = AR.AGRISN(+)
            and A.RULEISN      = DR.ISN(+)
            and A.ISN          = AD.AGRISN(+)
            and R.ISN          = GM.AGRISN(+)
            -- and AR.CLASSISN(+) = 430 -- страхователь
            and A.DISCR in ('Д', 'Г')
            and A.CLASSISN in ( select ISN
                                  from DICTI
                                 start with ISN = 34711216 -- тип договора страхования
                               connect by prior ISN = PARENTISN )
            AND  A.CLIENTISN=CLNT.ISN(+)
/* KGS  24.10.11 Нафиг не нужен тут этот гроупбай
          group by A.ISN, A.ID, A.DATEBEG, A.DATEEND, A.DATESIGN, A.CLASSISN, A.RULEISN,
                   A.DEPTISN, DR.FILTERISN, A.EMPLISN, A.CLIENTISN, A.CURRISN, A.INCOMESUM,
                   A.PREMIUMSUM, A.STATUS, A.DISCR, A.APPLISN, A.SHAREPC, A.REINSPC, A.GROUPISN,
                   A.BIZFLG, A.PARENTISN, LIMITSUM, INSUREDSUM, A.CREATED, A.FIRMISN, A.ADDRISN,
                   A.DATECALC */) S,
      REPAGRROLEAGR AR,
      REP_DEPT SD,
      REP_DEPT FIL,
      REP_DEPT BFIL
where S.DEPTISN   = SD.DEPTISN(+)
  and AR.EMITISN  = FIL.DEPTISN(+)
  and AR.BEMITISN = BFIL.DEPTISN(+)
  and S.AGRISN    = AR.AGRISN(+)
);
