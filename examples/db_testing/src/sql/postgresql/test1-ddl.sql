create schema test1;

create table test1.test1_table
(
    id                           serial primary key,
    name                         text                     not null,
    at                           timestamp with time zone,
    is_ok                        boolean                  not null,
    is_deleted                   boolean                  not null default false,
    created_at                   timestamp with time zone not null default now(),
    created_by_user_id           varchar(255)             not null default '',
    created_by_user_nameid       varchar(255)             not null default '',
    last_modified_at             timestamp with time zone not null default now(),
    last_modified_by_user_id     varchar(255)             not null default '',
    last_modified_by_user_nameid varchar(255)             not null default ''
);

create table test1.test1_table2
(
    id        serial primary key,
    table2_id integer not null references test1.test1_table (id),
    name      text    not null
);

create view test1.v_test1_table2 as
select t2.*,
       t1.name as t1_name
from test1.test1_table2 t2
         join test1.test1_table t1 on t1.id = t2.table2_id;
