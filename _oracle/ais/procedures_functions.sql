FUNCTION         GETISUM2(pagrisn number,pcurrisn number,ppdate date:=null,
          poisn number :=null,prisn number:=null,limmode char:=null,pxisn number:=null,plimclassisn number:=null) return number is
pdate1 date;
pdate2 date;
pdate date;
vinsuredsum number;
vlimiteverysum number:=0;
w number:=0;
w0 number:=0;
acurrisn number;
BEGIN
   if nvl(poisn,0)=0 and nvl(prisn,0)=0 and limmode is null then
    select getCrossCover(decode(plimclassisn,411,0,limitSum)+decode(plimclassisn,412,0,insuredsum),CurrISN,pCurrISN,nvl(ppdate,datebeg)) into vinsuredsum
      from agreement where isn=pagrisn;
    return vInsuredSum;
   end if;
   select DateBeg, DateEnd, currisn into pDate1, pDate2, acurrisn
   from AGREEMENT where ISN=pAgrISN;
   if ppdate is null then pdate:=pdate1;
   else pdate:=ppdate;
   end if;
   if pDate > pDate2 then
      pDate1 := pDate2;
   elsif pDate >= pDate1 then
          pDate1 := pDate;
   end if;
  -- raise_application_error(-20010,'debug: '||pdate1);
   if nvl(poisn,0)=0 and nvl(prisn,0)=0 and limmode='Y' then
    for orec in (select level,isn from agrobject
                  start with agrisn=pagrisn and parentisn is null
                  connect by prior isn=parentisn ) loop
     select max(getCrossCover(limiteverySum,CurrISN,pCurrISN,pdate)) into w0
     from v_AGRCOND c
     where DateBeg <= pDate1
        and DateEnd >= pDate1
        and c.objisn = orec.isn
        and c.agrisn = pagrisn
        and limclassisn=nvl(plimclassisn,limclassisn)
        and limiteverysum>0;
     if orec.level>1 then w:=w+nvl(w0,0); else w:=nvl(w0,0); end if;
--     raise_application_error(-20001,orec.level||'-'||w0||'-'||w);
     if w>vlimiteverysum then vlimiteverysum:=w; end if;
    end loop;
    return vlimiteverysum;
   end if;
   if nvl(poisn,0)=0 and nvl(prisn,0)<>0 then
    if limmode='Y' then
    for orec in (select level,isn from agrobject
                  start with agrisn=pagrisn and parentisn is null
                  connect by prior isn=parentisn ) loop
     select max(getCrossCover(limiteverySum,CurrISN,pCurrISN,pdate)) into w0
     from v_AGRCOND c
     where DateBeg <= pDate1
        and DateEnd >= pDate1
        and c.objisn = orec.isn
        and c.agrisn = pagrisn
        and limclassisn=nvl(plimclassisn,limclassisn)
        and limiteverysum>0
        and c.riskisn in
        (select isn from agrrisk
         start with isn=prisn connect by prior isn=parentisn )
        and c.riskisn not in (select riskisn from agrxexcl where xisn=pxisn);
     if orec.level>1 then w:=w+w0; else w:=w0; end if;
     if w>vlimiteverysum then vlimiteverysum:=w; end if;
    end loop;
    return vlimiteverysum;
    else
     utl.riskisn:=prisn;
     utl.agrxisn:=pxisn;
     select sum(getCrossCover(limitSum,aCurrISN,pCurrISN,pdate)) into vInsuredSum
     from v_AGRCOND3 c
     where DateBeg <= pDate1
        and DateEnd >= pDate1
        and c.agrisn = pagrisn
        and limitsum>0
        and limclassisn=nvl(plimclassisn,limclassisn)
       /* and c.riskisn in
        (select isn from agrrisk
         start with isn=prisn connect by prior isn=parentisn )
        and c.riskisn not in (select riskisn from agrxexcl where xisn=pxisn)*/ ;
     return vInsuredSum;
    end if;
   end if;
   if nvl(poisn,0)<>0 and nvl(prisn,0)=0 then
    select sum(getCrossCover(limitSum,aCurrISN,pCurrISN,pdate)),
           sum(getCrossCover(limiteverySum,aCurrISN,pCurrISN,pdate)) into vInsuredSum,vlimiteverySum
    from v_AGRCOND_o c
    where DateBeg <= pDate1
        and DateEnd >= pDate1
        and c.agrisn = pagrisn
        and (limitsum>0 or limiteverysum>0)
        and limclassisn=nvl(plimclassisn,limclassisn)
        and c.objisn in
        (select isn from agrobject
         start with isn=poisn connect by prior isn=parentisn );
    if limmode='Y' then return vlimiteverysum; else return vInsuredSum; end if;
   end if;
   if limmode='Y' then
       for orec in (select level,isn from agrobject
                  start with isn=poisn
                  connect by prior isn=parentisn ) loop
     select max(getCrossCover(limiteverySum,CurrISN,pCurrISN,pdate)) into w0
     from v_AGRCOND c
     where DateBeg <= pDate1
        and DateEnd >= pDate1
        and c.objisn = orec.isn
        and c.agrisn = pagrisn
        and limiteverysum>0
        and limclassisn=nvl(plimclassisn,limclassisn)
        and c.riskisn in
        (select isn from agrrisk
         start with isn=prisn connect by prior isn=parentisn )
        and c.riskisn not in (select riskisn from agrxexcl where xisn=pxisn);
     if orec.level>1 then w:=w+w0; else w:=w0; end if;
     if w>vlimiteverysum then vlimiteverysum:=w; end if;
    end loop;
    return vlimiteverysum;
   else
    utl.riskisn:=prisn;
    utl.agrxisn:=pxisn;
    select sum(getCrossCover(limitSum,aCurrISN,pCurrISN,pdate)) into vInsuredSum
    from v_AGRCOND3 c
    where DateBeg <= pDate1
        and DateEnd >= pDate1
        and c.agrisn = pagrisn
     --   and limitsum>0
        and limclassisn=nvl(plimclassisn,limclassisn)
        and c.objisn in
        (select isn from agrobject
         start with isn=poisn connect by prior isn=parentisn )
       /* and c.riskisn in
        (select isn from agrrisk
         start with isn=prisn connect by prior isn=parentisn )
        and c.riskisn not in (select riskisn from agrxexcl where xisn=pxisn)*/ ;
    return vInsuredSum;
   end if;
END;


  CREATE OR REPLACE FUNCTION "AIS"."GETCROSSCOVER" (
 pAmount in number,  -- ����� � ������
 pCurrISN1 in number,  -- ISN �������� ������, ���� null - ��������� ���������
 pCurrISN2 in number,  -- ISN ������� ������, ���� null - ��������� ���������
 pDateVal in date,        -- ���� �������������
 pClassISN in number := c.RateCB, -- ��� ����� ��, ���� ...
 pCurrISN0 in number := c.LocalCurr,  -- ISN �����-������ ���������
 pAdjustDates in number := 0  -- 0 - ��������� ���� ������ � �����������
) return number DETERMINISTIC IS -- AL 1/11/05 - DETERMINISTIC
/* �������� �� ���������� �� ����� ������ � ������.
   ���� ���������������� �����-���� �����������, �� ������������ ����� ��������� ������.
   ��������� ������ ���� �� ���� ����, ����� ��������� ����������� ������ ��������� ����� ��������� ������.
   ��� ���������� ���� ����������� ������������ ����� ���������� ������:
   1. ������������ ���� ���.������ �� �������� ����
   2. ������������ ���� ���.������ �� ���� ���������� ����� ���.������
   3. ���� ���� ��������� ��������� �� ��������� (���� �� ����� ������),
      �������� ������������ ���� ���.������ �� ���� ���������� ����� ���.������
   4. ���� ���.������ - ������ (���� 2 ����� � ���� 1), ����� ����� 1 � 2
   5. ���� ���.������ - ������ (���� 2 ����� � ���� 3), ����� ����� 3 � 2
*/
vAmount number := nvl(pAmount,0);
cEuro constant number := 29448516;

-- ���� � ������ ��������
function getRate(
 pCurrISN1 in number,  -- ISN �������� ������
 pCurrISN2 in number,  -- ISN ������� ������
 pDateVal in out date  -- ���� �������������
) return number is     -- ���� � ������ ��������
vRate number;
begin
  select --+ INDEX_DESC(CURRATE  X_CURRATE_ISN12)
    DateVal, Rate/Scale into pDateval, vRate
  from CURRATE
  where ClassISN = pClassISN
    and CurrFromISN = pCurrISN1
    and CurrToISN = pCurrISN2
    and DateVal <= pDateVal
    and ROWNUM <= 1;
  return vRate;
end;

function EuroCross(pCurrIsn number) return boolean is
begin
  return  pCurrISN in  (
          64,  --LUF �������������� �����
          108, --ATS ����������� �������
          111, --BEF ����������� �����
          77,  --GRD ��������� ������
          97,  --IEP ���������� ����
          81,  --ESP ��������� ������
          99,  --ITL ����������� ����
          71,  --DEM �������� �����
          25,  --NLG ������������� �������
          31,  --PTE ������������� ������
          82,  --FIM ����������� �����
          --9912, --FIM ����������� ����� 07276902
          85   --FRF ����������� �����
       );
end;

function GetRateOptimize
(pCurrISN1 in number,  -- ISN �������� ������
 pCurrISN2 in number,  -- ISN ������� ������
 pDateVal  in date     -- ���� �������������
) return number is     -- ���� 
vRate1 number := 1;
vRate2 number := 1;
vRate3 number := 1;
vDateVal1 date := pDateVal;
vDateVal2 date := pDateVal;
vDateVal3 date;
vDateStart date := to_date ('01012002','DDMMYYYY');
begin
   if pCurrISN1 != pCurrISN0 then

       if pCurrISN2 != pCurrISN0 then
         begin
           return getRate(pCurrISN1, pCurrISN2, vDateVal1);
         exception when no_data_found then null; -- ����� ������� ����� �����-������
         end;

          -- ������� 22.01.2002 � ����� ���� �������� ������ ���������, �?
         begin
           return 1 / getRate(pCurrISN2, pCurrISN1, vDateVal1);
         exception when no_data_found then null; -- ����� ������� ����� �����-������
         end;
       end if;
       
       if pDateVal>=vDateStart and pClassIsn=480 and EuroCross(pCurrISN1) then
            vRate1 := getRate(cEuro, pCurrISN0, vDateVal1)/getRate(cEuro, pCurrIsn1, vDateStart);
       else vRate1 := getRate(pCurrISN1, pCurrISN0, vDateVal1);
       end if;
    end if;
    
    
    if pCurrISN2 != pCurrISN0 then
      if pDateVal>=vDateStart and pClassIsn=480 and EuroCross(pCurrISN2) then
            vRate2 := getRate(cEuro, pCurrISN0, vDateVal2)/getRate(cEuro, pCurrIsn2, vDateStart);
       else
         if pAdjustDates=0 then vDateVal2:=nvl(vDateVal1,pDateVal); else vDateVal2:=pDateVal; end if; -- AL 1/11/05 - task 1518743403

         vRate2 := getRate(pCurrISN2, pCurrISN0, vDateVal2);
         if (vDateVal2 < vDateval1) and (pAdjustDates = 0) then -- �������� �����-���� ��� ������ ������. ������� ��� vDateVal1=null

          vDateVal3 := vDateVal2;
          begin
            vRate3 := getRate(pCurrISN1, pCurrISN0, vDateVal3);
            if vDateVal2 - vDateVal3 < vDateVal1 - vDateval2 then -- ������ 2 ������, ��������� ������ 1 ���������� ����
              vRate1 := vRate3;
            end if;
          exception when no_data_found then null;
          end;
       end if;
--
     end if;
   end if;
   return vRate1 / vRate2;
end;



BEGIN
    if pDateVal is null    then return null;    end if;
    if nvl(pAmount,0)=0    then return pAmount; end if; -- ������� 25.07.01
    if pCurrISN1=pCurrISN2 then return vAmount; end if;

    begin
      return vAmount * GetRateOptimize(pCurrISN1, pCurrISN2, pDateVal);
    exception when others then null; -- no_data_found then null;
    end;

    /* ���� ����� �������� ����*/
    return vAmount / GetRateOptimize(pCurrISN2, pCurrISN1, pDateVal);

exception
  when no_data_found then --raise;
    raise_application_error(-20102,'GETCROSSCOVER: �� ������ �����-���� ������ �� '||to_char(pDateVal, 'DD-MON-YYYY')||' (CurrFrom='||pCurrISN1||',CurrTo='||pCurrISN2||')');
END;;