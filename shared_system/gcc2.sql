CREATE OR REPLACE FUNCTION shared_system.gcc2_load_pl() RETURNS void AS $$
    my $rv = spi_exec_query("select rate, to_char(dateval, 'yyyymmdd') dateval, currfromisn, currtoisn from ais.currate  where currfromisn in (35,53,29448516) and  currtoisn in (35,53,29448516) and dateval>=timestamp '2014-02-20' order by  currfromisn, currtoisn , dateval");
    foreach my $rn (0 .. $rv->{processed} - 1) {
        my $currkey = "$rv->{rows}[$rn]->{currfromisn}|$rv->{rows}[$rn]->{currtoisn}";
        $rv->{rows}[$rn]->{rate} =~ s/0*$//g;        
        $_SHARED{$currkey}{$rv->{rows}[$rn]->{dateval}} = $rv->{rows}[$rn]->{rate};       
        
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


CREATE or replace FUNCTION public.gcc2_setdata(p_currpair varchar, p_keys varchar[], p_values varchar[]) RETURNS void AS $$
begin
    perform shared_system.gcc2_setdata_pl(p_currpair, p_keys, p_values) from shared_system.segment_distributor;
    perform shared_system.gcc2_setdata_pl(p_currpair, p_keys, p_values);
end;
$$ LANGUAGE plpgsql
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
        perform shared_system.gcc2_setdata(v_currpair, v_keys, v_values);
    end loop;
end;
$$ LANGUAGE plpgsql
volatile;


CREATE OR REPLACE FUNCTION shared_system.gcc2(pAmount numeric,pCurIn numeric, pCurOut numeric, pDate timestamp) RETURNS numeric AS $$
    $_[3] =~ s/(\d\d\d\d)-(\d\d)-(\d\d).*/\1\2\3/g;
    return $_SHARED->{"$_[1]|$_[2]"}{$_[3]};
$$ LANGUAGE plperl
volatile;

/*
select gcc2_load_pl();
select shared_system.gcc2(123,53,35,timestamp '2014-02-20')
select gcc2_getcurrpairs()
select gcc2_getdata('53|35')
select gcc2_load()
select clear_shared()
select gcc2_setdata_pl('53|35', array['111','222'],array['0.111','0.222']) ;
select array['111','222']
select gcc2(123,53,35,timestamp '2014-02-20'), gp_segment_id from ais.currate limit 100;
*/