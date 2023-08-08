-- create

create table logs.to_csv_logs (
	date_log timestamptz,
	action varchar
);
commit;

-- drop

drop table logs.to_csv_logs;
commit;


-- actions with logs

select * from logs.to_csv_logs
order by date_log desc;

