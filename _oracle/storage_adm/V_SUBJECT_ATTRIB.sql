  CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_SUBJECT_ATTRIB" ("SUBJISN", "HOME_ADDRISN", "HOME_CITYISN", "HOME_REGIONISN", "HOME_ZIP", "POST_ADDRISN", "POST_CITYISN", "POST_REGIONISN", "TEMPORARY_ADDRISN", "TEMPORARY_CITYISN", "TEMPORARY_REGIONISN", "PASSPORT_ADDRISN", "PASSPORT_CITYISN", "PASSPORT_REGIONISN", "PASSPORT_ZIP", "PASSPORT_ADDRESS", "FACT_ADDRISN", "FACT_CITYISN", "FACT_REGIONISN", "JUR_ADDRISN", "JUR_CITYISN", "JUR_REGIONISN", "VIPCLASSISN", "SUBJSECURITYSTR", "ADDRESSSECURITYSTR", "PHONESECURITYSTR", "DRIVINGDATEBEG", "AGEGROUP", "CITIZENSHIP", "N_KIDS", "MARRIAGESTATEISN", "FAMILYSTATEISN", "STOADAYSPAY", "DRIVERST", "MOTIVATION", "NO_MAIL", "EMAIL", "SERV_PHONE", "MOBILEPHONE", "PHONE", "HOME_PHONE", "BIRTHDAY", "ADDRISN", "CITYISN", "REGIONISN", "STO_PRIORITY", "JURIDICAL", "SUBJ_CLASSISN", "SMS_PHONE", "DENY_INFO_SMS", "DENY_PROMO_SMS", "DENY_INFO_EMAIL", "DENY_PROMO_EMAIL", "AGENTCATEGORYISN", "BESTADDRISN", "BESTADDR", "MAINOKVEDISN", "CLIENTISARRESTED", "DENY_INFO_POST", "DENY_INFO_CALL", "MONITORINGISN", "MONITORINGBEG", "MONITORINGEND", "MONITORINGUPD") AS 
  with tt as (select ISN from tt_rowid)

select --+ ordered use_nl(S SJ OptAddr)
  S.SUBJISN,

  S.HOME_ADDRISN,
  S.HOME_CITYISN,
  S.HOME_REGIONISN,
  S.HOME_ZIP,

  S.POST_ADDRISN,
  S.POST_CITYISN,
  S.POST_REGIONISN,

  S.TEMPORARY_ADDRISN,
  S.TEMPORARY_CITYISN,
  S.TEMPORARY_REGIONISN,

  S.PASSPORT_ADDRISN,
  S.PASSPORT_CITYISN,
  S.PASSPORT_REGIONISN,
  S.PASSPORT_ZIP,
  S.PASSPORT_ADDRESS,

  S.FACT_ADDRISN,
  S.FACT_CITYISN,
  S.FACT_REGIONISN,

  S.JUR_ADDRISN,
  S.JUR_CITYISN,
  S.JUR_REGIONISN,

  S.VIPCLASSISN,
  S.SUBJSECURITYSTR,
  S.ADDRESSSECURITYSTR,
  S.PHONESECURITYSTR,
  S.DRIVINGDATEBEG,
  S.AGEGROUP,
  S.CITIZENSHIP,
  S.N_KIDS,
  S.MARRIAGESTATEISN,
  S.FAMILYSTATEISN,
  S.STOADAYSPAY,
  S.DRIVERST,
  S.MOTIVATION,
  S.NO_MAIL,  -- ������ �� �������������� ��������: Y - ���������, N - ���������
  S.EMAIL,
  S.SERV_PHONE,
  S.MOBILEPHONE,
  S.PHONE,
  S.HOME_PHONE,
  S.BIRTHDAY,
  S.AddrISN,
  S.CITYISN,
  S.REGIONISN,
  S.STO_PRIORITY,
  SJ.JURIDICAL,
  SJ.CLASSISN as SUBJ_CLASSISN,
  S.SMS_Phone,

  -- sts 09.04.2012
  -- ��� ����������� ��� ���������, ��� ��� �� ��� �������� :)
  -- ��. �-�� AIS.SMS.pGetSubjConsent - ������ ��� ���
  decode(SJ.CLASSISN, 497, 'N', Deny_Info_SMS) as Deny_Info_SMS,         -- ������ �� �������������� ���������� �� ���
  decode(SJ.CLASSISN, 497, 'N', Deny_Promo_SMS) as Deny_Promo_SMS,       -- ������ �� ��������� ���������� �� ���
  decode(SJ.CLASSISN, 497, 'N', Deny_Info_EMail) as Deny_Info_EMail,     -- ������ �� �������������� ���������� �� email
  decode(SJ.CLASSISN, 497, 'N', Deny_Promo_EMail) as Deny_Promo_EMail,   -- ������ �� ��������� ���������� �� email
  S.AgentCategoryISN,    -- ��������� ������

  -- ����������� ����� �� ������ �������
  OptAddr.AddrISN as BestAddrISN,  -- FK(SubAddr) - ISN
  OptAddr.SubAddr as BestAddr,     -- ������ ������

  S.MainOKVEDISN,  -- �������� ����� FK(Dicti)
  Q.CLIENTISARRESTED,  -- ������� ������� ��� �������

  -- VAA 11.10.2013 55417293503
  decode(SJ.CLASSISN, 497, 'N', Deny_Info_Post) as Deny_Info_Post,     -- ������ �� �������������� ���������� � ���� �������� �������� ��������
  decode(SJ.CLASSISN, 497, 'N', Deny_Info_Call) as Deny_Info_Call,     -- ������ �� �������������� ���������� � ���� ������

  -- kuzmin(04.12.2013) task(58045024503)
  -- ����������
  S.MonitoringISN,
  S.MonitoringBeg,
  S.MonitoringEnd,
  S.MonitoringUpd

from (
  select
    coalesce(O.O_SUBJISN, ADR.ADR_SUBJISN, PH.PH_SUBJISN, VIP.VIP_SUBJISN, SH.SH_SubjISN) as SUBJISN,

    O.DrivingDateBeg,
    O.AgeGroup,
    O.Citizenship,
    O.N_Kids,
    O.MarriageStateISN,
    O.FamilyStateISN,
    O.STOADaysPay,
    O.DriverSt,
    O.Motivation,
    O.STO_Priority,
    O.AgentCategoryISN,
    O.MainOKVEDISN,  -- �������� �����

    nvl(O.No_Mail, 'N') as No_Mail,  -- ������ �� �������������� ��������: Y - ���������, N - ��������� (�� ���������)
    -- ����������� SMS �������� �� �����:
    -- ������ �� �������������� ���������� �� ��� (�� ��������� ���������)
    nvl(O.Deny_Info_SMS, 'N') as Deny_Info_SMS,
    -- ������ �� ��������� ���������� �� ��� (�� ��������� ���������)
    nvl(O.Deny_Promo_SMS, 'Y') as Deny_Promo_SMS,
    -- ������ �� �������������� ���������� �� email (�� ��������� ���������)
    nvl(O.Deny_Info_EMail, 'N') as Deny_Info_EMail,
    -- ������ �� ��������� ���������� �� email (�� ��������� ���������)
    nvl(O.Deny_Promo_EMail, 'Y') as Deny_Promo_EMail,

    -- VAA 11.10.2013 55417293503
    -- ������ �� �������������� ���������� � ���� �������� �������� �������� (�� ��������� ���������)
    nvl(O.Deny_Info_Post, 'N') as Deny_Info_Post,
    -- ������ �� �������������� ���������� � ���� ������ (�� ��������� ���������)
    nvl(O.Deny_Info_Call, 'N') as Deny_Info_Call,



    ADR.*,
    PH.*,
    VIP.*,

    SH.BIRTHDAY,

    -- kuzmin(04.12.2013) task(58045024503)
    -- ����������
    O.MonitoringISN,
    O.MonitoringBeg,
    O.MonitoringEnd,
    O.MonitoringUpd

  from
    TT,
    -- OBJ_ATTRIB
    (select --+ ordered use_nl(tt oa) use_hash(D)
        oa.ObjISN as O_SubjISN,

        max(decode(D.isn, 2647785103, oa.ValD)) as DrivingDateBeg,
        max(decode(D.isn, 1686027703, oa.Val)) as AgeGroup,
        max(decode(D.isn, 2200008503, oa.Val)) as Citizenship,
        max(decode(D.isn, 2626755703, oa.ValN)) as N_Kids,
        max(decode(D.isn, 2626755803, oa.ValN)) as MarriageStateISN,
        max(decode(D.isn, 3028738303, oa.ValN, 2638580803, oa.ValN)) as FamilyStateISN,
        max(decode(D.isn, 1343855503, oa.ValN)) as STOADaysPay,
        max(decode(D.isn, 1686031603, oa.Val)) as DriverSt,
        max(decode(D.isn, 1428587803, oa.ValN)) as Motivation,
        max(decode(D.isn, 1683459803, oa.ValN)) as STO_Priority,
        max(decode(D.isn, 2291578303, oa.ValN)) as AgentCategoryISN,  -- ��������� ������
        max(decode(D.isn, 3994769103, oa.ValN)) as MainOKVEDISN,  -- �������� �����

        -- �������/���������� ��� �������� �� �����
        -- sts 09.04.2012 - ������ ��� ���� No_Mail ���� �� ������� (��. SYSTEM.DDL_UNDO_TEXT).
        -- ������ ������ ������ �������� ���������
        -- � ��� ���� ������������ � ������� ��� �������� �� ���, �� ��� � �������, ������ ���������,
        -- ����� ������������ ���������� ��������.
        max(decode(D.isn, 3096320703, nvl2(oa.ValN, 'Y', 'N'))) as No_Mail,  -- ������ �� �������������� �������� (�� ��������� ���������)
        -- � ��� ��������� ����������� SMS �������� �� �����:
        -- ������ �� �������������� ���������� �� ���
        max(decode(D.isn, 3096320703, decode(oa.ValN, 3546162703, 'Y', 'N'))) as Deny_Info_SMS,
        -- ������ �� ��������� ���������� �� ���
        max(decode(D.isn, 2896523903, decode(oa.ValN, 3546162703, 'N', 'Y'))) as Deny_Promo_SMS,
        -- ������ �� �������������� ���������� �� email
        max(decode(D.isn, 3096320703, decode(oa.ValN, 3546162503, 'Y', 'N'))) as Deny_Info_EMail,
        -- ������ �� ��������� ���������� �� email
        max(decode(D.isn, 2896523903, decode(oa.ValN, 3546162503, 'N', 'Y'))) as Deny_Promo_EMail,

        -- VAA 11.10.2013 55417293503
        -- ������ �� �������������� ���������� � ���� �������� �������� ��������
        max(decode(D.isn, 3096320703, decode(oa.ValN, 3546162303, 'Y', 'N'))) as Deny_Info_Post,
        -- ������ �� �������������� ���������� � ���� ������
        max(decode(D.isn, 3096320703, decode(oa.ValN, 3546162103, 'Y', 'N'))) as Deny_Info_Call,

        -- kuzmin(04.12.2013) task(58045024503)
        -- ����������
        max(decode(D.isn, 3002827403, oa.ValN)) as MonitoringISN,
        max(decode(D.isn, 3002827403, oa.datebeg)) as MonitoringBeg,
        max(decode(D.isn, 3002827403, oa.dateend)) as MonitoringEnd,
        max(decode(D.isn, 3002827403, oa.updatedby)) as MonitoringUpd

      from
        tt,
        AIS.obj_attrib oa,
       (select
          connect_by_root d.isn as root,
          d.isn
        from dicti d
        start with d.isn in (
          2647785103,              -- ������������ ����
          1686027703,              -- ���������� ������
          2200008503,              -- �����������
          2626755703,              -- ���������� �����
          2626755803,              -- ���������� � �����
          3028738303, 2638580803,  -- �������� ���������
          1343855503,              -- ���� ������ ����
          1686031603,              -- ������������ ���� (���������),
          1428587803,              -- ������������� ������
          3096320703,              -- ������ �� �������������� ���������� - c.get('ATTRNoINFOFLAG')
          2896523903,              -- �������� �� ��������� ���������� - c.get('ATTRINFOFLAG')
          1683459803,              -- ��������� ��� �����������
          2291578303,              -- ��������� ������
          3994769103,              -- �������� �����
          3002827403               -- ����������
        )
        connect by prior d.isn = d.parentisn
        ) D
      where
        tt.isn = oa.ObjISN
        and oa.classisn = D.isn
        and oa.discr = 'C'
        and sysdate between nvl(oa.datebeg, sysdate) and nvl(oa.dateend, sysdate)
      group by oa.ObjISN
      ) O,
      -- SUBADDR
      ( select
          adr.SubjISN as Adr_SubjISN,
          max(decode(adr.ClassISN, 471, adr.AddrISN)) as Home_AddrISN,
          max(decode(adr.ClassISN, 471, adr.cityisn)) as Home_CityISN,
          max(decode(adr.ClassISN, 471, adr.regionisn)) as Home_RegionISN,
          max(decode(adr.ClassISN, 471, adr.postcode)) as Home_ZIP,

          max(decode(adr.ClassISN, 472, adr.AddrISN)) as Post_AddrISN,
          max(decode(adr.ClassISN, 472, adr.cityisn)) as Post_CityISN,
          max(decode(adr.ClassISN, 472, adr.regionisn)) as Post_RegionISN,

          max(decode(adr.ClassISN, 1166402903, adr.AddrISN)) as Temporary_AddrISN,
          max(decode(adr.ClassISN, 1166402903, adr.cityisn)) as Temporary_CityISN,
          max(decode(adr.ClassISN, 1166402903, adr.regionisn)) as Temporary_RegionISN,

          max(decode(adr.ClassISN, 11441319, adr.AddrISN)) as Passport_AddrISN,
          max(decode(adr.ClassISN, 11441319, adr.cityisn)) as Passport_CityISN,
          max(decode(adr.ClassISN, 11441319, adr.regionisn)) as Passport_RegionISN,
          max(decode(adr.ClassISN, 11441319, adr.postcode)) as Passport_ZIP,
          max(decode(adr.ClassISN, 11441319, adr.address)) as Passport_Address,

          max(decode(adr.ClassISN, 15522816, adr.AddrISN)) as Fact_AddrISN,
          max(decode(adr.ClassISN, 15522816, adr.cityisn)) as Fact_CityISN,
          max(decode(adr.ClassISN, 15522816, adr.regionisn)) as Fact_RegionISN,

          max(decode(adr.ClassISN, 470, adr.AddrISN)) as Jur_AddrISN,
          max(decode(adr.ClassISN, 470, adr.cityisn)) as Jur_CityISN,
          max(decode(adr.ClassISN, 470, adr.regionisn)) as Jur_RegionISN,

          -- ����� � ������ ��� V_BM (���� ����� ������������)
          max(adr.AddrISN) as AddrISN,
          max(adr.cityisn) as CityISN,
          max(adr.regionisn) as RegionISN
        from (
          select --+ ordered use_nl(adr cty reg)
            adr.AddrISN,
            adr.SubjISN,
            adr.ClassISN,
            adr.CityISN,
            nvl(decode(reg.ParentISN, 0, to_number(null), reg.ParentISN), reg.ISN) as RegionISN,
            adr.postcode,
            adr.address
          from
           (select --+ ordered use_nl(t adr)
              adr.subjisn,
              adr.classisn,
              max(adr.ISN) keep(dense_rank first order by adr.Updated desc, adr.ISN desc) as AddrISN,
              max(adr.cityisn) keep(dense_rank first order by adr.Updated desc, adr.ISN desc) as cityisn,
              max(adr.postcode) keep(dense_rank first order by adr.Updated desc, adr.ISN desc) as postcode,
              max(adr.address) keep(dense_rank first order by adr.Updated desc, adr.ISN desc) as address
            from
              tt t,
              ais.subaddr_t adr
            where
              t.ISN = adr.SubjISN
              and nvl(adr.ACTIVE, 'S') <> 'S'  -- ��������� ������ �������� ������
            group by adr.subjisn, adr.classisn
           ) adr,
             ais.city cty,
             ais.region reg
           where
             adr.cityisn = cty.isn(+)
             and cty.regionisn = reg.isn(+)
        ) adr
        group by adr.SubjISN
      ) Adr,
      -- SUBPHONE
      ( select --+ ordered use_nl(t P)
          Ph.*,
          decode(Ph.SMSPhone,
                 null, null,
                 AIS.SMS.pFormatPhone(Ph.SMSPhone)
          ) as SMS_Phone
        from
          ( select --+ ordered use_nl(t P dx)
              P.SubjISN as PH_SubjISN,
              -- e-mail
              max(decode(P.CLASSISN, 424, P.PHONE)) keep(dense_rank first order by decode(P.CLASSISN, 424, 0, 1), p.Updated desc) as EMAIL,
              -- ����. ���������
              max(decode(P.CLASSISN, 1482515703, P.PHONE)) keep(dense_rank first order by decode(P.CLASSISN, 1482515703, 0, 1), p.Updated desc) as SERV_PHONE,
              -- ���������
              max(decode(P.CLASSISN, 25152816, P.PHONE)) keep(dense_rank first order by decode(P.CLASSISN, 25152816, 0, 1), p.Updated desc) as MOBILEPHONE,
              -- �������
              max(decode(P.CLASSISN, 420, P.PHONE)) keep(dense_rank first order by decode(P.CLASSISN, 420, 0, 1), p.Updated desc) as PHONE,
              -- ��������
              max(decode(P.CLASSISN, 29155416, P.PHONE)) keep(dense_rank first order by decode(P.CLASSISN, 29155416, 0, 1), p.Updated desc) as HOME_PHONE,
              -- ���� ��� ��� ��������
              max(nvl2(dx.classisn1, P.Remark || P.Phone, null)) keep(dense_rank first order by nvl2(dx.classisn1, 0, 1), dx.classisn2, p.Updated desc) as SMSPhone
            from
              tt,
              AIS.subphone_t P,
              AIS.dicx dx
            where
              tt.ISN = P.SubjISN
              and dx.classisn(+) = 3378432503 --c.get('SMS_PHONE_TYPES')   -- ���� ��������� ��� ���-��������
              and p.ClassIsn = dx.classisn1(+)
            group by P.SubjISN
          ) Ph
      ) Ph,
      -- VIP
      ( select
          V.*
        from (
          select --+ ordered use_nl(t s sh sa sp) use_hash(VIP)
            s.ISN as VIP_SubjISN,
            max(VIP.ISN) as VipClassISN,
            max(sh.SecurityStr) as SubjSecurityStr,
            conc(distinct sa.SecurityStr) as AddressSecurityStr,
            conc(distinct sp.SecurityStr) as PhoneSecurityStr
          from
            tt_rowid t,
            ais.subject_t s,
            (select isn from dicti d start with isn in (11634718, 2431326703) connect by prior isn = parentisn) VIP,
            ais.subhuman_t sh,
            ais.subaddr_t sa,
            ais.subphone_t sp
          where
            t.ISN = s.ISN
            and S.ClassISN = VIP.ISN(+)
            and S.ISN = sh.ISN(+)
            and S.ISN = sa.SubjISN(+)
            and S.ISN = sp.SubjISN(+)
          group by
            s.ISN
         ) V
       where
         V.VipClassISN is not null
         or
         coalesce(V.SubjSecurityStr, V.AddressSecurityStr, V.PhoneSecurityStr) is not null
      ) VIP,
      (select --+ ordered use_nl(t SH)
         SH.ISN as SH_SubjISN,
         SH.BIRTHDAY, ROWNUM AS rn
       from
         tt_rowid t,
         AIS.SUBHUMAN_T SH
       where
         t.ISN = SH.ISN
         and SH.BIRTHDAY is not null   -- �������� ������ ������ �� ��������� ������������
      ) SH
  where
    TT.ISN = O.O_SUBJISN(+)
    and TT.ISN = ADR.ADR_SUBJISN(+)
    and TT.ISN = PH.PH_SUBJISN(+)
    and TT.ISN = VIP.VIP_SUBJISN(+)
    and TT.ISN = SH.SH_SubjISN(+)
  ) S,
    AIS.SUBJECT_T SJ,
    STORAGE_SOURCE.SUBJ_BEST_ADDR OptAddr,
    (select --+ ordered use_nl(t Q)
       -- sts 13.03.2013
       -- ��� ������ �� ���: AIS.AGRC.FCUR_WarnN
       -- ������ ��� ������������ ������, ����������� �� Q.DATESEND � �������� ������. �� ��� ������ �������� - ��
       -- ��� ���� max()
       Q.ObjISN as SUBJISN,
       max(decode(Q.REQUEST, '1', 'Y', 'N')) as CLIENTISARRESTED  -- ������� "�����"
     from tt_rowid t, AIS.QUEUE Q
     where
       t.ISN = Q.ObjISN
       and Q.ClassISN = 1175052903   -- ������������/������ ��� ������� (NEW) / C.Get('qeInArrestedNew')
       and Q.ObjISN2 is null
       and Q.FormISN = 33024916  -- ��. ���� / C.GET('fmLegal')
       and Q.Status = 'W'        -- ��� � ���
       and Q.Request = '1'       -- ���� ������ ���� ���� ��� �����, ������� ����� ������� ������ �����������
     group by Q.ObjISN
    ) Q

where
  S.SUBJISN = SJ.ISN
  and S.SUBJISN = OptAddr.SUBJISN(+)
  and S.SUBJISN = Q.SubjISN(+);