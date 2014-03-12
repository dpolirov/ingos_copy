create or replace view storage_adm.v_rep_agent_ranks (
   agrisn,
   addisn,
   is_move_obj_addisn,
   addid,
   orderno,
   agr_id,
   agr_datebeg,
   agr_dateend,
   agr_datesign,
   agr_ruleisn,
   add_datebeg,
   add_dateend,
   role_datebeg,
   role_dateend,
   agentisn,
   agentclassisn,
   addruleisn,
   is_move_obj,
   sharepc_agent_by_add,
   agent_sumclassisn,
   agent_sumclassisn2,
   agent_calcflg,
   agent_base,
   agent_baseloss,
   agent_planfact,
   agent_deptisn,
   rnk_move_obj,
   sharepc_by_add,
   cnt_agent_by_agr,
   cnt_agent_by_add,
   is_move_obj_id,
   is_move_obj_datebeg,
   is_move_obj_dateend,
   sharepc_by_is_move_obj,
   cnt_agent_by_is_move_obj,
   is_add_cancel,
   is_add_cancel_addisn,
   subjclassisn )
as
(
    with agr as (

	select --+ ordered use_nl(ra a)
                        t.agrisn,
                        t.agr_id,
                        t.agr_datebeg,
                        t.agr_dateend,
                        t.agr_datesign,
                        t.agr_ruleisn,
                        oracompat.nvl(t.datebeg::timestamp, t.agr_datebeg::timestamp) as datebeg,
                        lead(t.datebeg,1,t.agr_dateend) over (partition by t.agrisn order by t.datebeg, case when t.datebeg is null
                                                                                            then 1 else 0 end desc) as dateend,
                        decode(t.ruleisn, 37564716, 1, 0) as is_move_obj, -- перенос тс
                        decode(t.ruleisn, 34710416, 1, 0) as is_add_cancel, -- прекращение договора
                        oracompat.nvl(t.isn, t.agrisn) as addisn,
                        oracompat.nvl(t.id, t.agr_id) as addid,
                        oracompat.nvl(t.ruleisn, t.agr_ruleisn) as ruleisn
                      from
                      (
			select  ra.agrisn,
				ra.agr_id,
				ra.agr_datebeg,
				ra.agr_dateend,
				ra.agr_datesign,
				ra.agr_ruleisn,
				a.datebeg,
				a.ruleisn,
				a.isn,
				a.id			
                        from
                           (select --+ ordered use_nl(t ra) use_hash(carrules)
                                  t.isn as agrisn,
                                  -- атрибуты договора
                                  ra.id as agr_id,
                                  oracompat.trunc(ra.datebeg) as agr_datebeg,
                                  oracompat.trunc(ra.dateend) as agr_dateend,
                                  oracompat.trunc(ra.datesign) as agr_datesign,
                                  ra.ruleisn as agr_ruleisn,
                                  oracompat.nvl2(carrules.isn, ra.agrisn, null) as parentisn  -- будем искать аддендумы только для моторного страхования
                                from
                                      storage_adm.tt_rowid t
                                        inner join storage_source.repagr ra
                                        on t.isn = ra.agrisn
                                        left join motor.v_dicti_rule carrules
                                        on ra.ruleisn = carrules.isn
                                where ra.status in ('В', 'Д', 'Щ')
                          ) ra,
                            ais.agreement a
                      where
                          ra.agrisn = a.isn and a.discr = 'Д'  -- договор
                                                                                                    

                        ----
                        union
                        ----
			select  ra.agrisn,
				ra.agr_id,
				ra.agr_datebeg,
				ra.agr_dateend,
				ra.agr_datesign,
				ra.agr_ruleisn,
				a.datebeg,
				a.ruleisn,
				a.isn,
				a.id
                      from
                           (select --+ ordered use_nl(t ra) use_hash(carrules)
                                  t.isn as agrisn,
                                  -- атрибуты договора
                                  ra.id as agr_id,
                                  oracompat.trunc(ra.datebeg) as agr_datebeg,
                                  oracompat.trunc(ra.dateend) as agr_dateend,
                                  oracompat.trunc(ra.datesign) as agr_datesign,
                                  ra.ruleisn as agr_ruleisn,
                                  oracompat.nvl2(carrules.isn, ra.agrisn, null) as parentisn  -- будем искать аддендумы только для моторного страхования
                                from
                                      storage_adm.tt_rowid t
                                        inner join storage_source.repagr ra
                                        on t.isn = ra.agrisn
                                        left join motor.v_dicti_rule carrules
                                        on ra.ruleisn = carrules.isn
                                where ra.status in ('В', 'Д', 'Щ')) ra,
                            ais.agreement a
                      where
                        ra.parentisn = a.parentisn and a.discr = 'А'  -- аддендумы 
                        ) t
                        ) ,

add_move as (
                  select
                        a.agrisn,
                        a.addisn,
                        a.addid,
                        a.datebeg,
                        lead(a.datebeg, 1, a.agr_dateend) over (partition by a.agrisn order by a.datebeg) as dateend
                      from agr a
                      where
                        a.is_move_obj = 1
            ),
    add_cancel as (
                      select distinct
                            ad.root_isn,
                            first_value(ad.addisn) over (partition by ad.root_isn order by lv asc) as addisn
                          from (
                                 select t1.lv,
                                        t1.unh as root_isn,
                                        t1.addisn
                                    from (
                                            select unnest(ag.__hier) as unh,
                                                    shared_system.get_level(ag.__hier) as lv,
                                                    ag.isn,
                                                    oracompat.nvl(ag.previsn, ag.isn) as addisn
                                                from ais.agreement_nh_prev ag    
                                        ) as t1
                                        inner join (
                                                        select a.isn
                                                            from ais.agreement_nh_prev a
                                                            where isn in (select distinct agr.addisn 
                                                                                from agr 
                                                                                where agr.is_add_cancel = 1)
                                                ) as t2
                                        on t1.unh = t2.isn
                            ) ad
                )
          
    select --+ ordered use_nl(a s)
          a.agrisn,
          a.addisn,
          a.is_move_obj_addisn,
          a.addid,  -- номер аддендума (номер договора для договора)
          a.orderno,
          a.agr_id,       -- номер договора
          a.agr_datebeg,
          a.agr_dateend,
          a.agr_datesign,
          a.agr_ruleisn,
          a.add_datebeg,
          a.add_dateend,
          a.role_datebeg,
          a.role_dateend,
          a.agentisn,
          a.agentclassisn,
          a.addruleisn,
          a.is_move_obj,
          a.sharepc_agent_by_add,  -- процент комиссии по агенту
          a.agent_sumclassisn,
          a.agent_sumclassisn2,
          a.agent_calcflg,
          a.agent_base,
          a.agent_baseloss,
          a.agent_planfact,
          a.agent_deptisn,
          a.rnk_move_obj,
          a.sharepc_by_add,
          a.cnt_agent_by_agr,
          a.cnt_agent_by_add,
          a.is_move_obj_id,
          a.is_move_obj_datebeg,
          a.is_move_obj_dateend,
          a.sharepc_by_is_move_obj,
          a.cnt_agent_by_is_move_obj,
          a.is_add_cancel,  -- признак аддендума "прекращение договора"
          a.is_add_cancel_addisn,  -- ссылка на первый ненулевой аддендум/договор от аддендума "прекращение договора"
          s.classisn as subjclassisn
    from (
          select
                a.*,
                rank() over (partition by a.agrisn, decode(a.is_move_obj, 1, a.addisn, a.agrisn) order by a.orderno) as rnk_move_obj,
                sum(a.sharepc_agent_by_add) over (partition by a.agrisn, a.addisn) as sharepc_by_add,
                count(distinct a.orderno || ':' || a.agentisn) over (partition by a.agrisn) as cnt_agent_by_agr,
                count(distinct a.orderno || ':' || a.agentisn) over (partition by a.addisn) as cnt_agent_by_add,
                sum(a.sharepc_agent_by_add) over (partition by a.agrisn, a.is_move_obj_addisn) as sharepc_by_is_move_obj,
                count(distinct a.orderno || ':' || a.agentisn) over (partition by a.agrisn, a.is_move_obj_addisn) as cnt_agent_by_is_move_obj
          from (
                select
                      a.agrisn,
                      a.addisn,
                      ar.subjisn as agentisn,
                      ar.orderno,
                      max(a.agr_id) as agr_id,
                      max(a.agr_datebeg) as agr_datebeg,
                      max(a.agr_dateend) as agr_dateend,
                      max(a.agr_datesign) as agr_datesign,
                      max(a.agr_ruleisn) as agr_ruleisn,
                      max(a.addid) as addid,
                      max(a.datebeg) as add_datebeg,
                      max(a.dateend) as add_dateend,
                      max(a.ruleisn) as addruleisn,
                      max(a.is_move_obj) as is_move_obj, -- перенос тс
                      max(a.is_move_obj_addisn) as is_move_obj_addisn,   -- аддендум "перенос тс", к которому относится текущий аддендум (для первоначального состояния = agrisn)
                      max(a.is_move_obj_id) as is_move_obj_id,
                      max(a.is_move_obj_datebeg) as is_move_obj_datebeg,
                      max(a.is_move_obj_dateend) as is_move_obj_dateend,
                      max(a.is_add_cancel) as is_add_cancel,
                      max(a.is_add_cancel_addisn) as is_add_cancel_addisn,
                      min(oracompat.nvl(ar.datebeg::date, to_date('01-01-1900','dd-mm-yyyy'))) as role_datebeg,
                      max(oracompat.nvl(ar.dateend::date, to_date('01-01-3000','dd-mm-yyyy'))) as role_dateend,
                      max(ar.classisn) as agentclassisn,
                      sum(ar.sharepc) as sharepc_agent_by_add,
                      max(ar.sumclassisn) as agent_sumclassisn,
                      max(ar.sumclassisn2) as agent_sumclassisn2,
                      max(ar.calcflg) as agent_calcflg,
                      sum(ar.base) as agent_base,
                      sum(ar.baseloss) as agent_baseloss,
                      min(ar.planfact) as agent_planfact,  -- min - приоритет f - факт
                      max(ar.deptisn) as agent_deptisn
                    from
                        ( select
                                agr.*,
                                case 
                                    when agr.dateend - agr.datebeg < 1 
                                        then agr.dateend 
                                    else oracompat.trunc(agr.dateend) 
                                end as dateend_calc,
                                oracompat.nvl(add_move.addisn, agr.agrisn) as is_move_obj_addisn,
                                oracompat.nvl(add_move.addid, agr.agr_id) as is_move_obj_id,
                                oracompat.nvl(add_move.datebeg::timestamp, agr.agr_datebeg::timestamp) as is_move_obj_datebeg,
                                first_value(agr.dateend::timestamp - decode(agr.dateend, agr.agr_dateend, 0, 1) * interval '1 day') over (partition by oracompat.nvl(add_move.addisn, agr.agrisn) order by agr.datebeg desc) as is_move_obj_dateend,
                                oracompat.nvl(add_cancel.addisn, agr.addisn) as is_add_cancel_addisn
                              from agr
                                        left join add_move
                                        on agr.agrisn = add_move.agrisn
                                        and agr.datebeg >= add_move.datebeg
					and agr.datebeg < add_move.dateend
                                        left join add_cancel
                                        on agr.addisn = add_cancel.root_isn
                        ) a 
                            inner join ais.agrrole ar
                            on a.agrisn = ar.agrisn
                    where ar.classisn in (
                                            437,   -- агент
                                            2481446203, -- агент (бонусная комиссия)
                                            2530118403  -- генеральный агент
                                        )
                          and oracompat.nvl(ar.dateend::date, to_date('01-01-3000','dd-mm-yyyy')) >= a.datebeg
                          and oracompat.nvl(ar.datebeg::date, to_date('01-01-1900','dd-mm-yyyy')) < a.dateend_calc
                group by
                      a.agrisn,
                      a.addisn,
                      ar.subjisn,
                      ar.orderno
              ) a
    ) a
        left join ais.subject_t s
        on a.agentisn = s.isn
);
