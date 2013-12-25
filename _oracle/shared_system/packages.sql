  CREATE OR REPLACE PACKAGE "SHARED_SYSTEM"."GCC2" 
  IS

   type tCur3 is table of Number;
   type tCur2 is table of tCur3;
   type tCur is table of tCur2;
   CUR tCUR:=tCUR();
--procedure SetTable;
function GetIndex(pCurrisn in number:=NULL)return number;
function GetCurr(pIndex in number) return number;
FUNCTION repgetcrosscover
  ( pAmount in number,
    pCurrFrom in number,
    pCurrTo in number,
    pDate in date,
    pIsn in number:=null --ƒл€ протоколировани€ ошибок.
  )
  RETURN  number;

function gcc2(pAmount in number,pCurIn in number, pCurOut in number, pDate in date) return number PARALLEL_ENABLE;
procedure GetTable;
--procedure  Start_Up;
END; -- Package spec

CREATE OR REPLACE PACKAGE BODY "SHARED_SYSTEM"."GCC2" 
IS
-- SERPUKHOV
--decode(pCurrisn,35,1,53,2,29448516,3)
--decode(pCurrisn,1,35,2,53,3,29448516)
cDate date :=to_date('01.01.1990','dd.mm.yyyy');
vDays number:=trunc(sysdate)-cDate;
vCurIN number;
vCurOut number;

type tCurRec is record
(
cIn number,
cOut number,
cDate date,
cRate number
);
type tCurRate is table of tCurRec;

vCurRate tCurRate;



function GetIndex(pCurrisn in number:=NULL) return number is
begin
if pCurrisn is null then return 3;
   else
/*21,08,2007, красюков, чуш полна€*/

      return case pCurrisn when 35 then 1
                           when 53 then 2
                           when 29448516 then 3
                           else null end;
end if;
end;

function GetCurr(pIndex in number) return number is
begin
   return case pIndex when 1 then 35
                      when 2 then 53
                      when 3 then 29448516
                           else null end;
end;

FUNCTION repgetcrosscover
  ( pAmount in number,
    pCurrFrom in number,
    pCurrTo in number,
    pDate in date,
    pIsn in number:=null --ƒл€ протоколировани€ ошибок.
  )
  RETURN  number IS

BEGIN
   return getcrosscover(pAmount, pCurrFrom, pCurrTo, pDate);
EXCEPTION
   WHEN OTHERS  THEN
      IF TO_NUMBER(SQLCODE) BETWEEN -21000 AND -20000 THEN
         return null;
      ELSE
         RAISE;
      END IF;
END;

function gcc2(pAmount in number,pCurIn in number, pCurOut in number, pDate in date) return number
PARALLEL_ENABLE
is
vCurIn number;
vCurOut number;
begin
if nvl(round(pAmount,4),0)=0 then return pAmount; end if;

if pCurIn = pCurOut then return pAmount;end if;

if pCurIn is null or pCurOut is null or pDate is null then return null; end if;

/* sts - 16.04.2012 - вынес выше, чтобы если входна€ и выходна€ валюты равны, результат возвращалс€ как есть
   ≈сли же оставл€ть тут, то в случае отсутстви€ даты курса, возвращаетс€ null
if pCurIn = pCurOut then return pAmount;end if;
*/

vCurIn:=GetIndex(pCurIn);
vCurOut:=GetIndex(pCurOut);

if vCurIn+vCurOut >0
   and pDate-cDate>0 and pDate-cDate>=0  then
   begin
   return pAmount*CUR(vCurIn)(vCurOut)(least(vDays,trunc(pDate-cDate)));
   exception when others then
      if to_number(SQLCODE) =-6533 then

         return repgetcrosscover(pAmount,pCurIn, pCurOut, pDate);
      else
      raise;
      end if;

   end;
end if;
return repgetcrosscover(pAmount,pCurIn, pCurOut, pDate);
end;

procedure GetTable is
vCurIN number;
vCurOut number;
s varchar2(32700);
begin
if CUR.Count>0 then
--dbms_output.put_line('Level 1. Count: '||CUR.count);
   for rec1 in CUR.first..CUR.last loop
     --dbms_output.put_line('Level 2. Count: '||CUR(rec1).count);
         for rec2 in CUR(rec1).first..CUR(rec1).last loop
          --dbms_output.put_line('Level 3. Count: '||CUR(rec1)(rec2).count);
            --dbms_output.put_line(GetCurr(rec1)||' '||GetCurr(rec2));
            S:='';
            if rec1<>rec2 then
                for rec3 in CUR(rec1)(rec2).first..CUR(rec1)(rec2).last loop
                  S:=S||' '||TO_CHAR(ROUND(CUR(rec1)(rec2)(REC3),2));
                end loop;
            end if;
                --dbms_output.put_line(SUBSTR(S,1,255));
      end loop;
   end loop;
end if;
/* for cIn in 1..GetIndex() loop
    CUR.Extend;
    CUR(cIn):=tCur2();
      for cOut in 1..GetIndex() loop
         CUR(cIn).Extend;
         CUR(cIn)(cOut):=tCur3();
             if cIn<>cOut then
             vCurIN:=GetIndex(cIn);
             vCurOut:=GetIndex(cOut);
             select repgetcrosscover(1,
                                 vCurIN,
                                 vCurOut,
                                 cDate+rownum) bulk collect into CUR(cIn)(cOut) from
                                    repcond where Rownum <=vDays;
            end if;
      end loop;
   end loop;
*/
end;
procedure  Start_Up is
pragma AUTONOMOUS_TRANSACTION;
vDate date;
begin
select nvl(min(dateval),sysdate - 100)  into vDate from STORAGES.rep_currate where rownum <=1;
if sysdate - vDate  >1 then
delete from STORAGES.rep_currate;
if CUR.Count>0 then
   for rec1 in CUR.first..CUR.last loop
         for rec2 in CUR(rec1).first..CUR(rec1).last loop
            CUR(rec1)(rec2).delete;
      end loop;
      CUR(rec1).delete;
   end loop;
CUR.delete;
end if;
 for cIn in 1..GetIndex() loop
    CUR.Extend;
    CUR(cIn):=tCur2();
      for cOut in 1..GetIndex() loop
         CUR(cIn).Extend;
         CUR(cIn)(cOut):=tCur3();
             if cIn<>cOut then
             vCurIN:=GetCurr(cIn);
             vCurOut:=GetCurr(cOut);
             select repgetcrosscover(1,
                                 vCurIN,
                                 vCurOut,
                                 cDate+rownum) bulk collect into CUR(cIn)(cOut) from
                                    dicti where Rownum <=vDays;

               insert into STORAGES.rep_currate
                  select vCurIn, vCurOut, cDate+rownum, RepGetRate(cIn,cOut,rownum),
                  sysdate
                    from dicti where Rownum <=vDays
                    ;

---            dbms_output.put_line('LOAD: '||cIn||' '||cOut||'  '||sql%rowcount);
            end if;
      end loop;
   end loop;
   commit;

else
 vDays := trunc(vDate)-cDate; -- EGAO 25.11.2013
 for cIn in 1..GetIndex() loop
    CUR.Extend;
    CUR(cIn):=tCur2();
      for cOut in 1..GetIndex() loop
         CUR(cIn).Extend;
         CUR(cIn)(cOut):=tCur3();
         CUR(cIn)(cOut).Extend(vDays);
         end loop;
    end loop;



   select cIn, cOut, cDate, cRate bulk collect into vCurRate from STORAGES.rep_currate;

   for rec in vCurRate.first..vCurRate.Last loop
--   dbms_output.put_line(GetIndex(vCurRate(rec).cIn)||' '||GetIndex(vCurRate(rec).cOut)||' '||
--   trunc(to_number(vCurRate(rec).cDate-cDate)));

      CUR(GetIndex(vCurRate(rec).cIn))(GetIndex(vCurRate(rec).cOut))
      (trunc(to_number(vCurRate(rec).cDate-cDate))):=(vCurRate(rec).cRate);
   end loop;
end if;
 end;
 /*
||
||
||  Package StartUp Code;
||
*/
begin
Start_Up;
END;
;