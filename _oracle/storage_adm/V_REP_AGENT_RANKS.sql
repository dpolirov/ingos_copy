 CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_REP_AGENT_RANKS" ("AGRISN", "ADDISN", "IS_MOVE_OBJ_ADDISN", "ADDID", "ORDERNO", "AGR_ID", "AGR_DATEBEG", "AGR_DATEEND", "AGR_DATESIGN", "AGR_RULEISN", "ADD_DATEBEG", "ADD_DATEEND", "ROLE_DATEBEG", "ROLE_DATEEND", "AGENTISN", "AGENTCLASSISN", "ADDRULEISN", "IS_MOVE_OBJ", "SHAREPC_AGENT_BY_ADD", "AGENT_SUMCLASSISN", "AGENT_SUMCLASSISN2", "AGENT_CALCFLG", "AGENT_BASE", "AGENT_BASELOSS", "AGENT_PLANFACT", "AGENT_DEPTISN", "RNK_MOVE_OBJ", "SHAREPC_BY_ADD", "CNT_AGENT_BY_AGR", "CNT_AGENT_BY_ADD", "IS_MOVE_OBJ_ID", "IS_MOVE_OBJ_DATEBEG", "IS_MOVE_OBJ_DATEEND", "SHAREPC_BY_IS_MOVE_OBJ", "CNT_AGENT_BY_IS_MOVE_OBJ", "IS_ADD_CANCEL", "IS_ADD_CANCEL_ADDISN", "SUBJCLASSISN") AS 
  with AGR as (
-- ������ �� �������, ����������� (�� ���� ������) � ���������� (������)
  select --+ ordered use_nl(RA A)
    RA.AGRISN,
    -- �������� ��������
    RA.AGR_ID,
    RA.AGR_DATEBEG,
    RA.AGR_DATEEND,
    RA.AGR_DATESIGN,
    RA.AGR_RULEISN,
    -- �������� ��������� (��� ���������� �������� - ���� ��������)
    /* sts 01.11.2012 - �������� �����, ��� ���� ��������, � ������� ��������� ����������� �� ������: AgrISN = 176551249603
    nvl(trunc(A.DATEBEG), RA.AGR_DATEBEG) as DATEBEG,
    trunc(lead(A.DATEBEG, 1, RA.AGR_DATEEND) over(partition by RA.AGRISN order by A.DATEBEG nulls first)) as DATEEND,
    */
    nvl(A.DATEBEG, RA.AGR_DATEBEG) as DATEBEG,
    lead(A.DATEBEG, 1, RA.AGR_DATEEND) over(partition by RA.AGRISN order by A.DATEBEG nulls first) as DATEEND,

    decode(A.RULEISN, 37564716, 1, 0) as IS_MOVE_OBJ, -- ������� ��
    decode(A.RULEISN, 34710416, 1, 0) as IS_ADD_CANCEL, -- ����������� ��������
    -- ������ �� �������/��������, � �������� ��������� �����
    nvl(A.ISN, RA.AGRISN) as ADDISN,
    nvl(A.ID, RA.AGR_ID) as ADDID,
    nvl(A.RULEISN, RA.AGR_RULEISN) as RULEISN

  from
   (select --+ ordered use_nl(T RA) use_hash(CarRules)
      T.ISN as AGRISN,
      -- �������� ��������
      RA.ID as AGR_ID,
      trunc(RA.DATEBEG) as AGR_DATEBEG,
      trunc(RA.DATEEND) as AGR_DATEEND,
      trunc(RA.DATESIGN) as AGR_DATESIGN,
      RA.RULEISN as AGR_RULEISN,
      nvl2(CarRules.ISN, RA.AGRISN, null) as PARENTISN  -- ����� ������ ��������� ������ ��� ��������� �����������
    from
      TT_ROWID T,
      STORAGE_SOURCE.REPAGR RA,
      MOTOR.V_DICTI_RULE CarRules  -- ����������� �� ���������� ������ ��� ��������� ����������� (���������� REPBUH2OBJ)
    where
      T.ISN = RA.AGRISN
      and RA.STATUS in ('�', '�', '�')
      and RA.RULEISN = CarRules.ISN(+)

      --and ra.agrisn = 170415.1158

  ) RA,
    AIS.AGREEMENT A
  where
    (
      (RA.AGRISN = A.ISN and A.DISCR = '�')  -- �������
      or
      (RA.PARENTISN = A.PARENTISN and A.DISCR = '�')  -- ���������
    )
),
ADD_MOVE as (
-- ���������� ������ ���������� ��������/�������, ������� ��������� �����
-- ����� ��� ������������� ����� ��������� ������ � ������ "����� �������" task(���-12-2-166348, 32609379103)
  select
    A.AGRISN,
    A.ADDISN,
    A.ADDID,
    A.DATEBEG,
    lead(A.DATEBEG, 1, A.AGR_DATEEND) over(partition by A.AGRISN order by A.DATEBEG) as DATEEND
  from AGR A
  where
    A.IS_MOVE_OBJ = 1
),
ADD_CANCEL as (
  select
    AD.ROOT_ISN,
    max(AD.ADDISN) keep(dense_rank last order by lv) as ADDISN
  from (
    select
      level lv,
      CONNECT_BY_ROOT AD.ISN as ROOT_ISN,
      nvl(AD.PREVISN, AD.ISN) ADDISN
    from AGREEMENT AD
    start with AD.ISN in (select distinct AGR.ADDISN from AGR where AGR.IS_ADD_CANCEL = 1)
    connect by AD.ISN = prior AD.PREVISN and AD.PREMIUMSUM = 0
  ) AD
  group by AD.ROOT_ISN
)


/* �������
insert into tt_rowid(ISN) values(90913884603);  -- ������� �� ��� ���� ������
insert into tt_rowid(ISN) values(124407495003);  -- ������� �� � ����� ������
insert into tt_rowid(ISN) values(161772051303);  -- ���� ����� ��� ����, ��� ���, ���� - ������� ��
insert into tt_rowid(ISN) values(125000002403);  -- ������� �� � ����� ������, ��� ���-�� � �������
insert into tt_rowid(ISN) values(21604144103);   -- ��� ������ ��� ���, ��� ��-�� �� ������� �� � �������
-----------
AgrISN:
1. 183965.1158 - ������� � ����� ������� (���� ���������) (���� �������, �� ��� ������)
   170415.1158 - ������� � ����� ������� (��� ��������)
   85490139803 - ������� � ����� ������� (���� ������� ������� ��). ����� ����� ����������� ������� � ��������

2. 16410501003 - 4 ������, ��� ���������

3. 417636.2692 - ��� ������ (�� ������ ��������)
   9542896025  - 4 ������, 2 ��������

4. 62609209507 - ��� ������ � ����������������� ������, ���� ������� ��
   62609209507 - ��� ������ � ����������������� ������, ���� ������� �� (��� ��������)
   59276237103 - ��� ������ (+ 1 �����-�������� ��������), ���� ������� ��  <<-������� ������

���� ����� ��������� ��� ����������� � ��������:
4603910000 - �������
5909640000 - ����� - ���� ����� ��� ���� (������ ��� - �� ��-� ��������� �������). SharePC = 10
101506.05  - ����� - ���� ����� ��� ���� (��� �-�� ��������� �������). SharePC = 10
120823.1498 - �����, 3 ������, �� ��� ��� ����������. ���� ������� ��������� �������
8281029025 - �����, 3 ������, �� ��� ��� ����������. ��� ���������

*/


select --+ ordered use_nl(A S)
  A.AGRISN,
  A.ADDISN,
  -- �������� "������� ��", � �������� ��������� ������� �������� (��� ��������������� ��������� = AgrISN)
  -- ����� ��� �� ������, �.�. ��� ���������, ����������� ����� ��������� "������� ��"
  -- ��������� ������ ��������� ��������, ������������ ���������� "������� ��".
  -- � ��� - �� ���������� ��������� "������� ��"
  A.IS_MOVE_OBJ_ADDISN,
  A.ADDID,  -- ����� ��������� (����� �������� ��� ��������)
  A.ORDERNO,
  A.AGR_ID,       -- ����� ��������
  A.AGR_DATEBEG,
  A.AGR_DATEEND,
  A.AGR_DATESIGN,
  A.AGR_RULEISN,
  A.ADD_DATEBEG,
  A.ADD_DATEEND,
  A.ROLE_DATEBEG,
  A.ROLE_DATEEND,
  A.AGENTISN,
  A.AGENTCLASSISN,
  A.ADDRULEISN,
  A.IS_MOVE_OBJ,
  A.SHAREPC_AGENT_BY_ADD,  -- ������� �������� �� ������
  A.AGENT_SUMCLASSISN,
  A.AGENT_SUMCLASSISN2,
  A.AGENT_CALCFLG,
  A.AGENT_BASE,
  A.AGENT_BASELOSS,
  A.AGENT_PLANFACT,
  A.AGENT_DEPTISN,

  -- ���������� ����� ������ �� ��������� ������ ������ "����� �������" - task(���-12-2-166347)
  A.RNK_MOVE_OBJ,
  -- ����� % �������� �� ���������
  A.SHAREPC_BY_ADD,

  A.CNT_AGENT_BY_AGR,
  A.CNT_AGENT_BY_ADD,

  -- �������� ��� ��������� IS_MOVE_OBJ_ADDISN
  A.IS_MOVE_OBJ_ID,
  A.IS_MOVE_OBJ_DATEBEG,
  A.IS_MOVE_OBJ_DATEEND,

  -- ����� % �������� �� �������� ���������� � ��������� "������� ��"
  A.SHAREPC_BY_IS_MOVE_OBJ,
  -- ���-�� ������� �� �������� ���������� � ��������� "������� ��"
  A.CNT_AGENT_BY_IS_MOVE_OBJ,

  A.IS_ADD_CANCEL,  -- ������� ��������� "����������� ��������"
  A.IS_ADD_CANCEL_ADDISN,  -- ������ �� ������ ��������� ��������/������� �� ��������� "����������� ��������"
  S.CLASSISN as SUBJCLASSISN
from (
  select
    A.*,
    -- ���������� ����� ������ �� ��������� ������ ������ "����� �������" - task(���-12-2-166347)
    rank() over(partition by A.AGRISN, decode(A.IS_MOVE_OBJ, 1, A.ADDISN, A.AGRISN) order by A.ORDERNO) as RNK_MOVE_OBJ,
    /*
    -- ����� % �������� �� ���������/��������
    sum(A.SHAREPC_AGENT_BY_ADD)
      over(partition by A.AGRISN, decode(A.IS_MOVE_OBJ, 1, A.ADDISN, A.AGRISN)
           order by A.ORDERNO ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        as SHAREPC_BY_ADD,  -- todo - �������� ��� ������� AgrISN = 5909640000
    */
    -- ����� % �������� �� ���������
    sum(A.SHAREPC_AGENT_BY_ADD) over(partition by A.AGRISN, A.ADDISN) as SHAREPC_BY_ADD,

    count(distinct A.ORDERNO || ':' || A.AGENTISN) over(partition by A.AGRISN) as CNT_AGENT_BY_AGR,
    count(distinct A.ORDERNO || ':' || A.AGENTISN) over(partition by A.ADDISN) as CNT_AGENT_BY_ADD,

    -- ����� % �������� �� �������� ���������� � ��������� "������� ��"
    sum(A.SHAREPC_AGENT_BY_ADD) over(partition by A.AGRISN, A.IS_MOVE_OBJ_ADDISN) as SHAREPC_BY_IS_MOVE_OBJ,
    -- ���-�� ������� �� �������� ���������� � ��������� "������� ��"
    count(distinct A.ORDERNO || ':' || A.AGENTISN) over(partition by A.AGRISN, A.IS_MOVE_OBJ_ADDISN) as CNT_AGENT_BY_IS_MOVE_OBJ
  from (
    select
      -- �����
      A.AGRISN,
      -- ������ �� �������/��������, � �������� ��������� ����� (�� ������� ��������)
      A.ADDISN,
      AR.SUBJISN as AGENTISN,
      AR.ORDERNO,

      -- ��������
      max(A.AGR_ID) as AGR_ID,
      max(A.AGR_DATEBEG) as AGR_DATEBEG,
      max(A.AGR_DATEEND) as AGR_DATEEND,
      max(A.AGR_DATESIGN) as AGR_DATESIGN,
      max(A.AGR_RULEISN) as AGR_RULEISN,
      max(A.ADDID) as ADDID,
      max(A.DATEBEG) as ADD_DATEBEG,
      max(A.DATEEND) as ADD_DATEEND,
      max(A.RULEISN) as ADDRULEISN,
      max(A.IS_MOVE_OBJ) as IS_MOVE_OBJ, -- ������� ��
      max(A.IS_MOVE_OBJ_ADDISN) as IS_MOVE_OBJ_ADDISN,   -- �������� "������� ��", � �������� ��������� ������� �������� (��� ��������������� ��������� = AgrISN)
      -- �������� ��� ��������� IS_MOVE_OBJ_ADDISN
      max(A.IS_MOVE_OBJ_ID) as IS_MOVE_OBJ_ID,
      max(A.IS_MOVE_OBJ_DATEBEG) as IS_MOVE_OBJ_DATEBEG,
      max(A.IS_MOVE_OBJ_DATEEND) as IS_MOVE_OBJ_DATEEND,

      -- �������� ��� ��������� IS_ADD_CANCEL
      max(A.IS_ADD_CANCEL) as IS_ADD_CANCEL,
      max(A.IS_ADD_CANCEL_ADDISN) as IS_ADD_CANCEL_ADDISN,

      -- ���� �� AgrRole. ����������� �� AgentISN � ������ ���������/�������� ��� ���������� "�������" ������
      /* �����, �.�. �� AgrISN = 13141920003 ���� ����� ��������� ��� ���� � ������� ������� � ��������� ��� �� ��������,
      ��� � �� �������. �� �� �����, ���� ���������� �� SubjISN ���������� ������ ���������� �������� ��� �� � �:
      -- ������ - �� ��/�, ����� - ���� ��������� �
      -- ������� ����� � group by
      --min(abs(AR.ORDERNO)) as ORDERNO,
      */
      min(nvl(AR.DATEBEG, '01-jan-1900')) as ROLE_DATEBEG,
      max(nvl(AR.DATEEND, '01-jan-3000')) as ROLE_DATEEND,
      max(AR.CLASSISN) as AGENTCLASSISN,
      sum(AR.SHAREPC) as SHAREPC_AGENT_BY_ADD,
      max(AR.SUMCLASSISN) as AGENT_SUMCLASSISN,
      max(AR.SUMCLASSISN2) as AGENT_SUMCLASSISN2,
      max(AR.CALCFLG) as AGENT_CALCFLG,
      sum(AR.BASE) as AGENT_BASE,
      sum(AR.BASELOSS) as AGENT_BASELOSS,
      min(AR.PLANFACT) as AGENT_PLANFACT,  -- min - ��������� F - ����
      max(AR.DEPTISN) as AGENT_DEPTISN

    from
    ( select
        AGR.*,
        /* sts 01.11.2012
        �� �� ����, ��� ���� ��������, � ������� ��������� ����������� �� ������ (AgrISN = 176551249603)
        � ����� (�� ������� ��������) ����� ��������� ���� �������� � ����� ������ �������� 30.08.2012 ��
        ����������� ��� ����������� �� �������� � (� �������� ��������� 30.08.2012 15:13:00)
        ���������� ���� ��������� ������� ��������� �������:
        ���� ������ �������� ��������� ������ ������ ���, �� ���� ��������� �� �������
        (��� ��� ��� ����, ����� � ������ ��������� ��������� �������� 1).
        ����� - ������� �� ��� ����� ����� ��������� �� ������������ � �������� �
        � ��� � ���� ���������� ����� ���������� ������ �������� ���� ������
        */
        case when AGR.DATEEND - AGR.DATEBEG < 1 then AGR.DATEEND else trunc(AGR.DATEEND) end as DATEEND_CALC,
        -- ���������� �������� "������� ��", � �������� ��������� ������� ��������.
        -- ����� ��� �� ������, �.�. ��� ���������, ����������� ����� ��������� "������� ��"
        -- ��������� ������ ��������� ��������, ������������ ���������� "������� ��".
        -- � ��� - �� ���������� ��������� "������� ��"
        nvl(ADD_MOVE.ADDISN, AGR.AGRISN) as IS_MOVE_OBJ_ADDISN,
        nvl(ADD_MOVE.ADDID, AGR.AGR_ID) as IS_MOVE_OBJ_ID,
        nvl(ADD_MOVE.DATEBEG, AGR.AGR_DATEBEG) as IS_MOVE_OBJ_DATEBEG,
        first_value(AGR.DATEEND - decode(AGR.DATEEND, AGR.AGR_DATEEND, 0, 1)) over(partition by nvl(ADD_MOVE.ADDISN, AGR.AGRISN) order by AGR.DATEBEG desc) as IS_MOVE_OBJ_DATEEND,
        -- ���������� ������ ���������� ��������/�������, ������� ��������� �����
        -- ����� ��� ������������� ����� ��������� ������ � ������ "����� �������" task(���-12-2-166348, 32609379103)
        nvl(ADD_CANCEL.ADDISN, AGR.ADDISN) as IS_ADD_CANCEL_ADDISN
      from
        AGR,
        ADD_MOVE,
        ADD_CANCEL
      where
        AGR.AGRISN = ADD_MOVE.AGRISN(+)
        and AGR.DATEBEG >= ADD_MOVE.DATEBEG(+)
        and AGR.DATEBEG < ADD_MOVE.DATEEND(+)
        and AGR.ADDISN = ADD_CANCEL.ROOT_ISN(+)
    ) A,
      AIS.AGRROLE AR
    where
      -- 1. ������ �������� �����. �� �� ����� �� �������� ��������/�������� ��� �������
      -- ���: AgrISN = 183965.1158 ����� ������ ������ � ���� ��������, �� ���� �������� ������ � ������� �� ��������,
      -- ������� �� ������ �������� ���� ������ (�� �������������� ��������� ��������)
      -- �������� ��� ���������...
      -- 2. ��� ������������� �������� AgrISN = 133574884903 ���� ������������� (� ������ ��������) �����.
      -- �� �.�. ���� ��������� ��������� � ������ ��� ���� ������ ���������� ��������, �� �� ����� ��������
      -- ����� ���������� ����������� �� �������� 1 (��������� �������)
      -- �������� ��� ���� ���������, �.�. ���������� ����� � "�������" ��������
      A.AGRISN = AR.AGRISN
      and AR.CLASSISN in (
        437,   -- �����
        2481446203, -- ����� (�������� ��������)
        2530118403  -- ����������� �����
        /* � ����� - ���� ����� - ����� �������� ������� MOTOR.AGRAGENT_RANKS (��� ������� ����) ��� ��� ������ ������ (� ��������� �� ������������)
           ������ ������ �������� �� ����� ���������� ����. REP_AGENT_RANKS

        -- sts 12.10.2012 - ����� ��� ������� "���-�� �������� ����" � "��������� ����� (�� ���)" � Cognos
        -- ��� ������ "������������ ������ (�������)" �� �����, �� � ������ ������ ������� �����������������,
        -- ��� ��� ���������� ������� �� ������ �������� �� �����
        438   -- ������
        */
      )
      /* sts 01.11.2012 - �������� �����, ��� ���� ��������, � ������� ��������� ����������� �� ������: AgrISN = 176551249603
      and nvl(trunc(AR.DATEEND), '01-jan-3000') >= trunc(A.DATEBEG)
      and nvl(trunc(AR.DATEBEG), '01-jan-1900') < trunc(A.DATEEND)
      */
      and nvl(AR.DATEEND, '01-jan-3000') >= A.DATEBEG
      and nvl(AR.DATEBEG, '01-jan-1900') < A.DATEEND_CALC
    group by
      A.AGRISN,
      A.ADDISN,
      AR.SUBJISN,
      AR.ORDERNO
  ) A
) A,
  AIS.SUBJECT_T S
where
  A.AGENTISN = S.ISN(+);
 