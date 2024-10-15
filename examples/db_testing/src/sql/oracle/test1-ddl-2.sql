

CREATE TABLE "TEST1_TABLE"
(
    "ID"                           NUMBER(1)
    /*    name                         VARCHAR2(4000)           NOT NULL,
    at                           TIMESTAMP WITH TIME ZONE,
    is_ok                        NUMBER(1)                NOT NULL,
    is_deleted                   NUMBER(1)                DEFAULT 0 NOT NULL,
    created_at                   TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    created_by_user_id           VARCHAR2(255)            DEFAULT '' NOT NULL,
    created_by_user_nameid       VARCHAR2(255)            DEFAULT '' NOT NULL,
    last_modified_at             TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    last_modified_by_user_id     VARCHAR2(255)            DEFAULT '' NOT NULL,
    last_modified_by_user_nameid VARCHAR2(255)            DEFAULT '' NOT NULL*/
);

CREATE TABLE test1_table2
(
    id        NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    table2_id NUMBER                NOT NULL,
    name      VARCHAR2(4000)        NOT NULL,
    CONSTRAINT fk_test1_table FOREIGN KEY (table2_id) REFERENCES test1_table(id)
);

CREATE VIEW v_test1_table2 AS
SELECT t2.*,
       t1.name AS t1_name
FROM test1_table2 t2
         JOIN test1_table t1 ON t1.id = t2.table2_id;