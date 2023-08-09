-- в fill_account_turnover_f замены:
--  1 to_date дублировалось
--  2 вместо nullif coalesce в credit_amount_rub и debet_amount_rub (nlv)



-- Управление pg_cron

-- m h dm mm dw
select  cron.schedule('turnover', '57 * * * *', 
	'do $$ 
	begin
		for i in 1..31 loop
		 call ds.fill_account_turnover_f(
			to_date(concat(''2018-01-'', i) , ''yyyy-mm-dd'')
		 );
		end loop;	
	end;$$'
);

select cron.schedule('f101', '58 * * * *', 
	'call dm.fill_f101_round_f(to_date(''2018-01-01'', ''yyyy-mm-dd''))'
);


select * from cron.job
order by jobid desc;

select * from cron.job_run_details;

-- удаление jobs
select cron.unschedule('turnover');
select cron.unschedule('f101');


-- результат работы пакетов

select * from dm.lg_messages lm ;

select * from dm.dm_account_turnover_f
order by on_date ;

select
 from_date         
             to_date           
           , chapter           
           , ledger_account    
           , characteristic    
           , balance_in_rub    
           , balance_in_val    
           , balance_in_total  
           , turn_deb_rub      
           , turn_deb_val      
           , turn_deb_total    
           , turn_cre_rub      
           , turn_cre_val      
           , turn_cre_total    
           , balance_out_rub  
           , balance_out_val   
           , balance_out_total 
from dm.dm_f101_round_f dfrf ;


-- очистка таблиц
truncate dm.lg_messages;
truncate dm.dm_account_turnover_f;
truncate dm.dm_f101_round_f;
delete from cron.job_run_details ;
commit;









