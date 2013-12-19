CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_REP_AGR_SALERS" ("AGRISN", "SALERISN", "AGRSALERCLASSISN", "DEPTISN", "DEPT0ISN", "DEPT1ISN", "DOISN", "OISN", "IS_SALERGO", "IS_SALERF", "SALERCLASSISN", "DATEBEG", "DATEEND") AS 
  select --+ ordered use_nl(S oa) use_hash(RD)
-- sts 24.07.2012 - ����� �� ��������� (������ TT_AGR_SALERS)
-- ������������ ��������� �� AgrISN � tt_rowid � ���������. ������������� ������� �/� ����� V_REP_AGR_SALERS_MAX_DATES
   --S.pLoadISN,
   S.agrisn,
   S.SalerISN,
   S.AgrSClass as AGRSALERCLASSISN,
   S.deptisn,
   rd.Dept0ISN,
   rd.Dept1ISN,
   rd.DOISN,
   rd.OISN,
   S.IS_SALERGO,
   S.IS_SALERF,
   -- ������������� ������. ��������� ��� �������, � ������� �������� ��������� ���� ������ � ���������
   max(oa.valn) keep (dense_rank FIRST
                    order by case
                               when oa.datebeg is not null and oa.dateend is not null then 2
                               else case
                                      when oa.datebeg is not null then 1
                                      else case
                                             when oa.dateend is not null then 1
                                             else 0
                                           end
                                    end
                             end desc,
                             nvl(oa.datebeg, s.pMinDate) desc,
                             nvl(oa.dateend, s.pMaxDate) desc
                   ) AS SALERCLASSISN,
   s.dt as datebeg,
   s.dte as dateend
from
 (select
    a.*
  from
   (select
      a.*,
      -- ���������� ����� ��������� (��� ���� ������ ���������� ��������� ����� 1 ����)
      first_value(a.dt) over(partition by a.agrisn, a.SalerISN, a.AgrSClass
        order by a.dt range between 1 following and unbounded following) - 1 as dte
    from
      ( select
        distinct
          --rol.pLoadISN,
          rol.pMinDate,
          rol.pMaxDate,
          rol.SalerISN,
          rol.AgrISN,
          rol.AgrSClass,
          rol.role_datebeg,
          rol.role_dateend,
          rol.DeptISN,
          rol.IS_SALERGO,
          rol.IS_SALERF,
          oa.ValN,
          nvl(oa.datebeg, rol.pMinDate) as oa_datebeg,
          nvl(oa.dateend, rol.pMaxDate) as oa_dateend,
          decode(
            n.n,
            1, rol.role_datebeg,
            2, rol.role_dateend + 1,
            3, nvl(oa.datebeg, rol.pMinDate),
            4, nvl(oa.dateend, rol.pMaxDate) + 1
          ) as dt
        from
           ( select --+ ordered use_nl(s ar)
             distinct
               --s.pLoadIsn,
               s.pMinDate,
               s.pMaxDate,
               Ar.Agrisn,
               ar.SubjISN as SalerISN,
               ar.classisn as AgrSClass,
               Ar.DeptISN,
               Max(s.IS_SALERGO) over (partition by ar.AgrISN) as IS_SALERGO,
               Max(s.IS_SALERF) over (partition by ar.AgrISN) as IS_SALERF,
               nvl(trunc(ar.datebeg), s.pMinDate) as role_datebeg,
               nvl(trunc(ar.dateend), s.pMaxDate) as role_dateend
             from (-- ����� ������ ����� - �������� 1-�� �������� �� ������ �� ������ ��������.
                   -- � ��������, ����� ����� ����� �� ������, ���� ������ � ���� ��������� ����������. ;-)
                   select --+ ordered use_nl(ra ar) use_hash(SLR)
                   distinct
                     --ra.PLOADISN,
                     ra.PMINDATE,
                     ra.PMAXDATE,
                     First_Value(ar.isn)
                       over (partition by Ar.Agrisn, trunc(ar.datebeg), trunc(ar.dateend), ar.DeptIsn -- OD 29.10.2010 /*CASE WHEN sD.SHORTNAME LIKE  '������%' THEN SD.pARENTiSN ELSE SD.ISN END*/
                         order by
                           (case when ar.Classisn in (1738885603, 1738885903, 1738886903) then 1 else 0 end ) desc,
                            decode(ar.classisn, 1738886903, 1, 0), -- ������������� ����� ������������ ����� ��, ����� ��� ������
                            ar.UPDATED
                       ) ARISN,
                     decode(SLR.CODE, 'SALES_G', 'Y', 'N') as IS_SALERGO,
                     decode(SLR.CODE, 'SALES_F', 'Y', 'N') as IS_SALERF
                   FROM
                     V_REP_AGR_SALERS_MAX_DATES ra,
                     agrrole ar,
                    ( select
                        D.ISN, D.CODE
                      from
                        AIS.DICTI D
                      where
                        D.PARENTISN = 402   -- �������� ��������
                        and D.CODE in ('SALES_G', 'SALES_F')
                    ) SLR
                   WHERE
                     ra.AgrISN = ar.AgrISN
                     and ar.classisn = SLR.ISN
                     and ar.deptisn is not null
                ) s,
                  AgrRole ar
                where
                  s.ARISN = ar.isn
            ) rol,
              obj_attrib oa,
              -- ��������� ������ ������, ����� ��������� ������ ���� (beg-end �� ����� � ���������) � ���� �������
              (select rownum n from dicti where rownum <=4) n
            where
              rol.SalerISN = oa.objisn(+)
              and oa.classisn(+) = 1428587803
              and oa.discr(+) = 'C'
         ) a
       ) a
     where
     /*
       �� ���������� �������� ��������� �� ����, ������� ��������� �� �������� �������� �������
       sts - �� ��� ������ - �� �������������, �� ������� ������, �.�. ������� ������� ����� �������,
       ��� ���� ������� �� ����� ����������� �� ��������� �����. ������ ����� �� ���,
       ����� ��������� "���������" �������� �� "���������" �������� - �.�. ���� ������� �/� trunc()

       upd 24.07.2012 - � ����� � �������������. ������� �� ������� ���������� �� a.dt ���� ���� ������ ����� �������:
         200 944 639  -- ������� ���������
         200 952 028  -- ��������� ���������
       ������ ���� ��� ���� � trunc()-���� ��� - ����������� �� ����
       a.dt between a.role_datebeg and a.role_dateend
     */
       a.dt + (a.dte - a.dt) / 2 between a.role_datebeg and a.role_dateend
   ) s,
     ais.obj_attrib oa,
     rep_dept rd
  where
    s.SalerISN = oa.objisn(+)
    and oa.classisn(+) = 1428587803
    and oa.discr(+) = 'C'
    and s.dt between nvl(trunc(oa.datebeg(+)), s.pMinDate) and nvl(trunc(oa.dateend(+)), s.pMaxDate)
    and s.DeptISN = rd.DeptISN(+)
group by
   --S.pLoadISN,
   S.agrisn,
   S.SalerISN,
   S.AgrSClass,
   S.deptisn,
   rd.Dept0ISN,
   rd.Dept1ISN,
   rd.DOISN,
   rd.OISN,
   S.IS_SALERGO,
   S.IS_SALERF,
   s.dt,
   s.dte;