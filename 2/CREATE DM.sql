--------------------
-- Создание схемы DM
-- Создание сущностей для DM по схеме задания
-- Типа NUMBER в posgresql нет, аналогичный ему - NUMERIC

create schema DM;


create table DM.DM_ACCOUNT_TURNOVER_F (
	on_date DATE,
	account_rk NUMERIC,
	credit_amount NUMERIC(23,8),
	credit_amount_rub NUMERIC(23,8),
	debet_amount NUMERIC(23,8),
	debet_amount_rub NUMERIC(23,8)
);

create table DM.DM_F101_ROUND_F (
    FROM_DATE DATE,
    TO_DATE DATE,
    CHAPTER CHAR(1),
    LEDGER_ACCOUNT CHAR(5),
    CHARACTERISTIC CHAR(1),
    BALANCE_IN_RUB NUMERIC(23,8),
    R_BALANCE_IN_RUB NUMERIC(23,8),
    BALANCE_IN_VAL NUMERIC(23,8),
    R_BALANCE_IN_VAL NUMERIC(23,8),
    BALANCE_IN_TOTAL NUMERIC(23,8),
    R_BALANCE_IN_TOTAL NUMERIC(23,8),
    TURN_DEB_RUB NUMERIC(23,8),
    R_TURN_DEB_RUB NUMERIC(23,8),
    TURN_DEB_VAL NUMERIC(23,8),
    R_TURN_DEB_VAL NUMERIC(23,8),
    TURN_DEB_TOTAL NUMERIC(23,8),
    R_TURN_DEB_TOTAL NUMERIC(23,8),
    TURN_CRE_RUB NUMERIC(23,8),
    R_TURN_CRE_RUB NUMERIC(23,8),
    TURN_CRE_VAL NUMERIC(23,8),
    R_TURN_CRE_VAL NUMERIC(23,8),
    TURN_CRE_TOTAL NUMERIC(23,8),
    R_TURN_CRE_TOTAL NUMERIC(23,8),
    BALANCE_OUT_RUB NUMERIC(23,8),
    R_BALANCE_OUT_RUB NUMERIC(23,8),
    BALANCE_OUT_VAL NUMERIC(23,8),
    R_BALANCE_OUT_VAL NUMERIC(23,8),
    BALANCE_OUT_TOTAL NUMERIC(23,8),
    R_BALANCE_OUT_TOTAL NUMERIC(23,8)
);

create table dm.lg_messages (
	record_id INT not null,
	date_time TIMESTAMP,
	pid INT,
	message VARCHAR,
	message_type VARCHAR,
	usename VARCHAR, 
	datname VARCHAR, 
	client_addr VARCHAR, 
	application_name VARCHAR,
	backend_start TIMESTAMPTZ
);

CREATE SEQUENCE dm.seq_lg_messages
OWNED BY dm.lg_messages.record_id;

commit;


-- Удаление

drop schema if exists DM cascade;
drop table if exists DM.DM_ACCOUNT_TURNOVER_F cascade;
drop table if exists DM.DM_F101_ROUND_F cascade;	
drop table if exists dm.lg_messages cascade;
drop sequence if exists dm.seq_lg_messages;

commit;


