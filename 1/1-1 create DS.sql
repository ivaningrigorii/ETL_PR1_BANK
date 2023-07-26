--------------------
-- Создание схемы DS
create schema DS;
grant usage on schema DS to neoflex_user;
grant create on schema DS to neoflex_user;

-------------------
-- Создание сущностей для DS по схеме задания
-- Типа NUMBER в posgresql нет, аналогичный ему - NUMERIC

create table DS.FT_BALANCE_F (
  on_date DATE not null,
  account_rk NUMERIC not null,
  currency_rk NUMERIC,
  balance_out FLOAT,
  
  primary key (ON_DATE, ACCOUNT_RK)
);

create table DS.FT_POSTING_F (
  oper_date DATE not null,
  credit_account_rk NUMERIC not null,
  debet_account_rk NUMERIC not null,
  credit_amount FLOAT,
  debet_amount FLOAT,
  
  primary key (OPER_DATE, CREDIT_ACCOUNT_RK, DEBET_ACCOUNT_RK)
);

create table DS.MD_ACCOUNT_D (
  data_actual_date DATE not null,
  data_actual_end_date DATE not null,
  account_rk NUMERIC not null,
  account_number VARCHAR(20) not null,
  char_type VARCHAR(1) not null,
  currency_rk NUMERIC not null,
  currency_code VARCHAR(3) not null,
  
  primary key (DATA_ACTUAL_DATE, ACCOUNT_RK)
);

create table DS.MD_CURRENCY_D (
  currency_rk NUMERIC not null,
  data_actual_date DATE not null,
  data_actual_end_date DATE,
  currency_code VARCHAR(3),
  code_iso_char VARCHAR(3),
  
  primary key (CURRENCY_RK, DATA_ACTUAL_DATE)
);

create table DS.MD_EXCHANGE_RATE_D (
  data_actual_date DATE not null,
  data_actual_end_date DATE,
  currency_rk NUMERIC not null,
  reduced_cource FLOAT,
  code_iso_num VARCHAR(3),
  
  primary key (DATA_ACTUAL_DATE, CURRENCY_RK)
);


create table DS.MD_LEDGER_ACCOUNT_S (
  chapter CHAR(1),
  chapter_name VARCHAR(16),
  section_number INTEGER,
  section_name VARCHAR(22),
  subsection_name VARCHAR(21),
  ledger1_account INTEGER,
  ledger1_account_name VARCHAR(47),
  ledger_account INTEGER not null,
  ledger_account_name VARCHAR(153),
  characteristic CHAR(1),
  is_resident INTEGER,
  is_reserve INTEGER,
  is_reserved INTEGER,
  is_loan INTEGER,
  is_reserved_assets INTEGER,
  is_overdue INTEGER,
  is_interest INTEGER,
  pair_account VARCHAR(5),
  start_date DATE not null,
  end_date DATE,
  is_rub_only INTEGER,
  min_term VARCHAR(1),
  min_term_measure VARCHAR(1),
  max_term VARCHAR(1),
  max_term_measure VARCHAR(1),
  ledger_acc_full_name_translit VARCHAR(1),
  is_revaluation VARCHAR(1),
  is_correct VARCHAR(1),
  
  primary key (LEDGER_ACCOUNT, START_DATE)
);


--drop table if exists DS.FT_BALANCE_F ;
--drop table if exists DS.FT_POSTING_F ;
--drop table if exists DS.MD_ACCOUNT_D ;
--drop table if exists DS.MD_CURRENCY_D ;
--drop table if exists DS.MD_EXCHANGE_RATE_D ;
--drop table if exists DS.MD_LEDGER_ACCOUNT_S ;

select * from ds.ft_balance_f fbf ;

select * from ds.md_account_d mad ;
select * from ds.md_currency_d mcd ;
select * from ds.md_exchange_rate_d merd ;
select * from ds.md_ledger_account_s mlas ;

insert into ds.ft_balance_f (on_date, account_rk, currency_rk, balance_out)
values ('2020-01-01', 12, 23, 23.4);

select * from ds.MD_ACCOUNT_D;

select * from ds.ft_posting_f
where 
	  oper_date = '2018-01-09' and
	  credit_account_rk = 1761691 and 
	  debet_account_rk = 13630;


--------------------
-- Таблица LOGS
create schema LOGS;

create table LOGS.

	



