/*Temporary function, emulates c.get()*/
create or replace function shared_system.get(pName varchar) returns numeric as $BODY$
begin
    if upper(pName) = 'ADDCANCEL' then return 34710416;
    elsif upper(pName) = 'AGRMOTORCOMPULSORY' then return 753518300;
    elsif upper(pName) = 'ATTRLIQUIDATION' then return 2453825203;
    elsif upper(pName) = 'OPARTQUIT' then return 686696616;
    elsif upper(pName) = 'XDEPTREPROPER' then return 704062916;
    else return 0;
    end if;
end;
$BODY$
language plpgsql;
