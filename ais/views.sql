create view ais.buhhead as
select * from ais.buhhead_t where planisn=5;

create view ais.buhbody as
select * from ais.buhbody_t where planisn=5;

create view ais.buhsubacc as
select * from ais.buhsubacc_t where classisn=5;

create view ais.docs as
select * from ais.docs_t where planisn=5;

create view ais.subdept as
select * from ais.subdept_t where;

create view ais.subhuman as
select * from ais.subhuman_t;

create view ais.subject as
select * from ais.subject_t;