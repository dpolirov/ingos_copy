
  CREATE OR REPLACE FORCE VIEW "STORAGE_ADM"."V_REP_SUBJECT" ("SUBJISN", "CLASSISN", "ROLECLASSISN", "COUNTRYISN", "BRANCHISN", "JURIDICAL", "RESIDENT", "VIP", "INN", "ID", "FID", "CODE", "SHORTNAME", "FULLNAME", "ACTIVE", "UPDATED", "UPDATEDBY", "LICENSENO", "LICENSEDATE", "OKPO", "OKOHX", "SYNISN", "CREATEDBY", "CREATED", "PROFITTAXFLAG", "PARENTISN", "NAMELAT", "ORGFORMISN", "REMARK", "KPP", "SEARCHNAME", "SECURITYLEVEL", "OGRN", "OKVED", "SECURITYSTR", "REGNM", "SBCLASS", "RELICEND", "LICEND", "NREZNAME", "LIKVIDSTATUS", "R_BEST", "R_FITCH", "R_MOODYS", "R_SP", "R_WEISS", "ADDRCODE", "ADDRTYPE", "CITYISN", "POSTCODE", "ADDRESS", "PARENTSUBJ", "PARENTFULLNAME", "PARENTCLASS", "PARENTINN", "UPDATEDBY_NAME", "BNK_VKEY", "BNK_VKEYDEL", "BNK_ACTIVE", "VALAAM_NAME", "REGNMDTEND", "LIKVIDSTATUSDTEND", "CURATOR", "CURATORDEPTISN", "DEALER", "REPSYNKISN") AS 
  (
Select --+ Ordered Use_Nl(PSb Sbb PsbCl UpdBy Cr)  optimizer_features_enable('11.1.0.6') 
 S."ISN" subjisn ,S."CLASSISN",S."ROLECLASSISN",S."COUNTRYISN",S."BRANCHISN",
 S."JURIDICAL",S."RESIDENT",S."VIP",S."INN",S."ID",S."FID",S."CODE",
 S."SHORTNAME",S."FULLNAME",S."ACTIVE",S."UPDATED",S."UPDATEDBY",
Decode(S."LICENSENO",Null,
 (select Min(ExtId )from ais.subdoc sd where sd.subjisn=s.isn and classisn in (1735713203,974582025)),
 S."LICENSENO") "LICENSENO",

Decode(S."LICENSEDATE",Null,
 (select trunc( Min(SIGNED ) ) from ais.subdoc sd where sd.subjisn=s.isn and classisn in (1735713203,974582025)),
 S."LICENSEDATE")  LICENSEDATE,
 S."OKPO",S."OKOHX",S."SYNISN",
 S."CREATEDBY",S."CREATED",
 S."PROFITTAXFLAG",S."PARENTISN",
 S."NAMELAT",S."ORGFORMISN",S."REMARK",S."KPP",S."SEARCHNAME",S."SECURITYLEVEL",S."OGRN",S."OKVED",S."SECURITYSTR",S."REGNM",S."SBCLASS",S."RELICEND",S."LICEND",S."NREZNAME",S."LIKVIDSTATUS",S."R_BEST",S."R_FITCH",S."R_MOODYS",S."R_SP",S."R_WEISS",S."ADDRCODE",S."ADDRTYPE",S."CITYISN",S."POSTCODE",S."ADDRESS",S."PARENTSUBJ",
Psb.FullName ParentFullName,
PsbCl.ShortName ParentClass,
PSb.INN ParentINN,
UpdBy.FullName UPDATEDBY_NAME,

Sbb.VKEY BNK_VKEY,

Sbb.VKEYDEL BNK_VKEYDEL,

Sbb.ACTIVE  BNK_ACTIVE,
VALAAM_NAME,
REGNMDTEND,LikvidStatusDTEND,
Cr.ShortName Curator,
s.CrDeptisn CuratorDeptIsn,
s.Dealer,


              (
              case
                 when S.JURIDICAL  ='Y' and S.RESIDENT='Y' Then
                    (Case
         /*если филиал*/when    S.PARENTSUBJ is not null and S.INN=PSb.INN   Then
                          (Case /*если филиал и головная ликвидированна*/
                             When
                              (Select Max(Isn) from obj_attrib oa where oa.objisn=to_number(S.PARENTSUBJ) and oa.classisn=c.get('AttrLIQUIDATION') and Dateend<=sysdate ) Is not null
/*ais.u.getname(ais.utl.getnumberattrib( to_number(S.PARENTSUBJ),'LIQUIDATION','C',DATE # Sq(prompt ('Отчетная дата'; 'Date'))# )) is not null*/
                             then Nvl(ais.REINS_UTILS.GETSUCCESSOR(to_number(S.PARENTSUBJ),SYSdate ), to_number(S.PARENTSUBJ))
                           else
                            to_number(S.PARENTSUBJ) End
                           )
   /*если ликвидация*/When
/* ais.u.getname(ais.utl.getnumberattrib(S.SUBJISN,'LIQUIDATION','C',DATE #sq( prompt ('Отчетная дата'; 'Date'))# )) is not null  */
                         (Select Max(Isn) from obj_attrib oa where oa.objisn=S.ISN and oa.classisn=c.get('AttrLIQUIDATION') and Dateend<=sysdate) Is not null
                      Then Nvl(ais.REINS_UTILS.GETSUCCESSOR(S.ISN,sysdate ),S.Isn)
                   else
                      S.ISN End)
/* не резиденты : совпадают регномера и страна регистрации */
           when  S.RESIDENT='N' and  S.PARENTSUBJ is not null and S.REGNM = (Select Max(o.Val) keep (dense_rank last order by Nvl(Dateend,'01-jan-3000')) from ais.obj_attrib o where o.objisn=Psb.isn and  o.classisn=1813887503)
                 AND S.COUNTRYISN=PSB.COUNTRYISN
           THEN to_number(S.PARENTSUBJ)



             else S.isn
            end
              ) repsynkisn





from (

Select  --+ Ordered Use_Nl(Sc) Use_Merge(Sa)
 SB.*,
 (Select Max(o.Val) keep (dense_rank last order by Nvl(Dateend,'01-jan-3000')) from ais.obj_attrib o where o.objisn=SB.isn and  o.classisn=1813887503)   REGNM,
  (Select Max(o.DATEEND)  from ais.obj_attrib o where o.objisn=SB.isn and  o.classisn=1813887503)   REGNMDTEND,
 Sc.Shortname SBCLASS,
(select Nvl(Min(Nvl(Dateend,decode(isn,null,null,'01-jan-3000'))) ,'01-jan-1900')from ais.subdoc where subjisn=sb.isn and classisn=1735713203) ReLicEnd,
(select Nvl(Min(Nvl(Dateend,decode(isn,null,null,'01-jan-3000'))) ,'01-jan-1900')from ais.subdoc where subjisn=sb.isn and classisn=974582025) LicEnd,
 CASE  When RESIDENT  ='N' then Nvl(NAMELAT,Sb.FULLNAME ) else  Sb.FULLNAME end NRezName,
(Select Max(o.Val) from ais.obj_attrib o where o.objisn=SB.isn and  o.classisn=2453825203)   LikvidStatus,
  (Select Max(o.DATEEND) from ais.obj_attrib o where o.objisn=SB.isn and  o.classisn=2453825203)   LikvidStatusDTEND,

 (Select Max(o.Val) keep (dense_rank first  order by Nvl(Datebeg,'01-jan-1900') Desc) from ais.obj_attrib o where o.objisn=SB.isn and  o.classisn=1979965303)   R_BEST,
 (Select Max(o.Val) keep (dense_rank first  order by Nvl(Datebeg,'01-jan-1900') Desc)from ais.obj_attrib o where o.objisn=SB.isn and  o.classisn=1979962403)   R_FITCH,
 (Select Max(o.Val) keep (dense_rank first  order by Nvl(Datebeg,'01-jan-1900') Desc)from ais.obj_attrib o where o.objisn=SB.isn and  o.classisn=1979960103)   R_MOODYS,
 (Select Max(o.Val) keep (dense_rank first  order by Nvl(Datebeg,'01-jan-1900') Desc)from ais.obj_attrib o where o.objisn=SB.isn and  o.classisn=1979958603)   R_SP,
 (Select Max(o.Val)keep (dense_rank first  order by Nvl(Datebeg,'01-jan-1900') Desc) from ais.obj_attrib o where o.objisn=SB.isn and  o.classisn=1979966903)   R_WEISS,    
 
(Select Max(o.Val)keep (dense_rank first  order by Nvl(Datebeg,'01-jan-1900') Desc) from ais.obj_attrib o where o.objisn=SB.isn and  o.classisn=3019835703) VALAAM_NAME,

(Select Max(so.humanisn)keep(dense_rank first  order by so.Updated)
                                                              from subowner so, storage_source.rep_dept rd
                                                                            where  so.subjisn=SB.isn and so.deptisn = rd.deptisn
                                                                              and  (rd.dept1isn =3381054603 or rd.oisn = 1746865203 or rd.dept2isn in(3381054003,1393203203))
                                                                            ) CrIsn,
(Select Max(so.DeptIsn)keep(dense_rank first  order by so.Updated)
                                                              from subowner so, storage_source.rep_dept rd
                                                                            where  so.subjisn=SB.isn and so.deptisn = rd.deptisn
                                                                              and  (rd.dept1isn =3381054603 or rd.oisn = 1746865203 or rd.dept2isn in(3381054003,1393203203))
                                                                             ) CrDeptIsn,

(Select Max(o.Valn)keep (dense_rank first  order by Nvl(Datebeg,'01-jan-1900') Desc) from ais.obj_attrib o where o.objisn=SB.isn and  o.classisn=1693932103) Dealer,
 Sa.AddrCode,
 sa.AddrType,
 Sa.cityisn,
 Sa.postcode,
 Sa.address,
 Decode(Sb.Parentisn,Null,Null,
( Select /*+ optimizer_features_enable('11.1.0.6') */
    Isn
 from aIS.SubJect_T SSB
  --Where ParentIsn Is null
  Where Connect_By_ISLeaf=1
 Start With SSB.ISn=Sb.Parentisn
 Connect By Prior SSB.Parentisn=SSB.Isn and Nvl(SSB.Resident,'M')=Nvl(Sb.resident,'M')
)) ParentSubj

From TT_ROWID T,
 AIS.SUBJECT_T SB,ais.dicti SC,
( 
 select subjisn,
Max(dc.code) keep (dense_rank first  order by decode(cityisn,null,1,0), Dc.code,sa.isn) AddrCode,
Max(dc.shortname) keep (dense_rank first  order by decode(cityisn,null,1,0), Dc.code,sa.isn) AddrType,
Max(countryisn) keep (dense_rank first  order by decode(cityisn,null,1,0), Dc.code,sa.isn) countryisn,
Max(cityisn)keep (dense_rank first  order by decode(cityisn,null,1,0), Dc.code,sa.isn) cityisn,
Max(postcode) keep (dense_rank first  order by decode(cityisn,null,1,0), Dc.code,sa.isn) postcode,
Max(address) keep (dense_rank first  order by decode(cityisn,null,1,0), Dc.code,sa.isn) address
from TT_ROWID T,Ais.SUBADDR_T sa ,dicti dc
Where sa.classisn=dc.isn
AND T.ISN=SA.subjisn

Group by subjisn
) Sa
 Where  T.ISN=Sb.iSN
 AND Sc.Isn(+)=SB.classisn
 And Sb.isn=sa.subjisn(+)
 ) S,Ais.Subject_T PSb,SubBank Sbb,Dicti PsbCl, Ais.Subject_T UpdBy, Ais.Subject_t Cr
 Where S.ParentSubj=Psb.Isn(+)
 And S.Isn=Sbb.Isn(+)
 and PSb.classisn=PsbCl.Isn(+)
 and S.UpdatedBy=UpdBy.Isn(+)
 and s.CrIsn = Cr.Isn(+)
 );