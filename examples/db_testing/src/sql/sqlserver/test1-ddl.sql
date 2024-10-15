CREATE SCHEMA test1;

CREATE TABLE test1.test1_table
(
    id                           INT IDENTITY(1,1) PRIMARY KEY,
    name                         NVARCHAR(MAX)             NOT NULL,
    at                           DATETIME2(6),
    is_ok                        BIT                       NOT NULL,
    is_deleted                   BIT                       NOT NULL DEFAULT 0,
    created_at                   DATETIME2(6)              NOT NULL DEFAULT GETDATE(),
    created_by_user_id           NVARCHAR(255)             NOT NULL DEFAULT '',
    created_by_user_nameid       NVARCHAR(255)             NOT NULL DEFAULT '',
    last_modified_at             DATETIME2(6)              NOT NULL DEFAULT GETDATE(),
    last_modified_by_user_id     NVARCHAR(255)             NOT NULL DEFAULT '',
    last_modified_by_user_nameid NVARCHAR(255)             NOT NULL DEFAULT ''
);

CREATE TABLE test1.test1_table2
(
    id        INT IDENTITY(1,1) PRIMARY KEY,
    table2_id INT                NOT NULL REFERENCES test1.test1_table(id),
    name      NVARCHAR(MAX)      NOT NULL
);

CREATE VIEW test1.v_test1_table2 AS
SELECT t2.*,
       t1.name AS t1_name
FROM test1.test1_table2 t2
         JOIN test1.test1_table t1 ON t1.id = t2.table2_id;