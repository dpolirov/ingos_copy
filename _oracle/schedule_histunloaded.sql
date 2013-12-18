/*  Greenplum Replication   */
create or replace procedure GP_USER.FILL_HISTUNLOADED is
begin
declare             
    minisn number;
    maxisn number;
    vIsn NUMBER;
    vUnloadIsn NUMBER;
    SyncMask number;
begin
    -- Flagslow bit for GP server
    SyncMask := 256;
    
    -- Clean up replicated data from histunloaded
    loop
        delete /*+index(h)*/ from gp_user.histunloaded h where flagslow=0 and rownum<=10000;
        exit when sql%rowcount=0;
        commit;
    end loop;
    commit;

    -- Get left and right border of replicated HISTLOG part
    select min(isn)-1
        into minisn
        from (
            select /*+ index(h) */ isn
                from gp_user.histlog h
                where status ='ÿ'
                    and rownum <= 1000000
         );
    select /*+ index(h) */ max(isn)
        into maxisn
        from gp_user.histlog h;
    
    -- Loop of moving data from HISTLOG to HISTUNLOADED
    vISN := 0;
    loop
        -- Get next unloadisn
        select gp_user.seq_histunloaded.nextval
            into vUnloadIsn
            from dual;
        
        -- Get next chunk of HISTLOG data
        select max(isn)
            into vISN
            from (
                select /*+index_asc(p) */ isn
                    from gp_user.histlog p
                    where isn > minisn
                        and rownum <= 10000
                        and status ='ÿ'
                );
        exit when vISN is null;
        
        -- Move data to HISTUNLOADED
        insert into gp_user.histunloaded             
            select --+ index_asc(h PK_HISTLOG)
                    isn histisn, node, tablename, recisn, agrisn,
                    isn3, sessionid, transid, operation, vUnloadIsn unloadisn,
                    null flagfast, SyncMask flagslow, updated
                from gp_user.histlog h
                where status ='ÿ'
                    and isn > minisn
                    and isn <= vISN;
        
        -- Remove this data from HISTLOG
        delete --+ index(h PK_HISTLOG)
            from gp_user.histlog h
            where isn > minisn and isn <= vISN;
        
        commit;
        
        -- Move left border of isn interval
        minisn := vIsn;
        
        exit when vIsn > maxisn;             
    end loop;
end;
end;

declare
    x number;
begin
    dbms_job.submit(
        job        => x, 
        what       => 'GP_USER.FILL_HISTUNLOADED;',
        next_date  => sysdate,
        interval   => 'trunc(sysdate+1/144,''MI'')', 
        no_parse   => TRUE
    );
end;