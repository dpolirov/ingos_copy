-- Creates table with rates for each day starting from pDateStart
CREATE OR REPLACE FUNCTION shared_system.gcc2_fill_rep_currate(pDateStart timestamp) returns void as $$
begin
    truncate table storages.rep_currate;
    insert into storages.rep_currate(cin, cout, cdate, crate)
    select ad.currfromisn, ad.currtoisn, ad.dateval, r2.rate 
        from (
             select  c.dateval, r.currfromisn, r.currtoisn, max(r.dateval) dateavail
                from(
                    select current_date - generate_series dateval
                        from generate_series(0,extract(day from current_timestamp - pDateStart)::int)
                    ) c
                left join ais.currate r
                    on r.dateval<=c.dateval
                where r.codefrom in ('EUR','USD') 
                    and r.codeto in ('RUR','USD')
                    and r.codefrom <> r.codeto
                group by c.dateval, r.currfromisn, r.currtoisn
            )  ad
            inner join ais.currate r2 
                on  ad.currfromisn=r2.currfromisn 
                and ad.currtoisn=r2.currtoisn 
                and ad.dateavail=r2.dateval;
end;
$$ LANGUAGE plpgsql
volatile;

-- Get rates from storages.rep_currate and place them into hash. Runs on master
CREATE OR REPLACE FUNCTION shared_system.gcc2_load_pl() RETURNS void AS $$
    my $rv = spi_exec_query("select crate, to_char(cdate, 'yyyymmdd') cdate, cin, cout from storages.rep_currate order by  cin, cout, cdate");
    foreach my $rn (0 .. $rv->{processed} - 1) {
        my $currkey = "$rv->{rows}[$rn]->{cin}|$rv->{rows}[$rn]->{cout}";
        $rv->{rows}[$rn]->{crate} =~ s/0*$//g;        
        $_SHARED{$currkey}{$rv->{rows}[$rn]->{cdate}} = $rv->{rows}[$rn]->{crate};       
        
    }
$$ LANGUAGE plperl
volatile;


CREATE OR REPLACE FUNCTION shared_system.gcc2_getcurrpairs() RETURNS setof varchar AS $$
    foreach my $key (keys %_SHARED) {
        if ($key =~ '\d*\|\d*') {
            return_next($key);
        }
    }
    return ;
$$ LANGUAGE plperl
volatile;


CREATE OR REPLACE FUNCTION gcc2_getdata(currpair varchar) RETURNS setof varchar[] AS $$
    foreach my $key (keys %{$_SHARED{$_[0]}}) {
        return_next( [$key, $_SHARED{$_[0]}{$key}] );
    }    
    return ;
$$ LANGUAGE plperl
volatile;


CREATE or replace FUNCTION shared_system.gcc2_setdata_pl(p_currpair varchar, p_keys varchar[], p_values varchar[]) RETURNS void AS $$
    
    my @keys   = split(/,/, substr $_[1], 1, -1);
    my @values = split(/,/, substr $_[2], 1, -1);
    my $n = scalar @keys;
    foreach my $i (0 .. scalar @keys - 1) {
        $_SHARED{$_[0]}{@keys[$i]} = @values[$i];
    }
    return;
$$ LANGUAGE plperl
volatile;




CREATE OR REPLACE FUNCTION shared_system.gcc2_load() RETURNS void AS $$
declare
    v_keys   varchar[];
    v_values varchar[];
    v_currpair varchar;
begin
    perform shared_system.gcc2_load_pl();
    for v_currpair in (select * from  shared_system.gcc2_getcurrpairs()) loop
        select  array_agg(d[1]),
                array_agg(d[2])
            into v_keys,
                 v_values
            from shared_system.gcc2_getdata(v_currpair) as d;        
        perform shared_system.gcc2_setdata_pl(v_currpair, v_keys, v_values) from shared_system.segment_distributor;
    end loop;
end;
$$ LANGUAGE plpgsql
volatile;



CREATE OR REPLACE FUNCTION shared_system.gcc2_GetRateExact(pCurIn numeric, pCurOut numeric, pStrDate varchar(8)) RETURNS numeric AS $$    
    if (exists $_SHARED{"$_[0]|$_[1]"}){
        return  $_SHARED{"$_[0]|$_[1]"}{$_[2]};
    }
    return undef;
$$ LANGUAGE plperl
volatile;


CREATE OR REPLACE FUNCTION shared_system.gcc2_GetRate(pCurIn numeric, pCurOut numeric, pDate timestamp) RETURNS numeric AS $$
declare 
    vRate numeric;
begin
    vRate = shared_system.gcc2_GetRateExact(pCurIn, pCurOut, to_char(pDate,'YYYYMMDD'));
    if vRate is null then
        vRate = 1/shared_system.gcc2_GetRateExact(pCurOut, pCurIn, to_char(pDate,'YYYYMMDD'));
    end if;
    return vRate;
end;
$$ LANGUAGE plpgsql
volatile;


CREATE OR REPLACE FUNCTION shared_system.gcc2(pAmount in numeric,pCurIn in numeric, pCurOut in numeric, pDate in timestamp) returns numeric AS $$
declare 
    vRate numeric;
begin
    if coalesce(round(pAmount,4),0)=0 then return pAmount; end if;
    if pCurIn = pCurOut then return pAmount;end if;
    if pCurIn is null or pCurOut is null or pDate is null then return null; end if;
    vRate = shared_system.gcc2_GetRateExact(pCurIn, pCurOut, to_char(pDate,'YYYYMMDD'));
    if vRate is null then
        vRate = 1/shared_system.gcc2_GetRateExact(pCurOut, pCurIn, to_char(pDate,'YYYYMMDD'));
    end if;
    return pAmount*vRate;
end;
$$ LANGUAGE plpgsql
volatile;

