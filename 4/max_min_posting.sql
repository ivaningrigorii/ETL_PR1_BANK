/* *
 * Данная функция необходима 
 * для расчёта максимальных и минимальных
 * проводок по кредитам, по дебетам за день (i_OnDate). 
 * Данные по проводкам из ft_posting_f.
 * Данные для перевода из md_exchange_rate_d через md_account_d.
 * 
 */

create or replace function ds.min_max_posting (i_OnDate  date)
  returns table (
    date_posting date,
    min_posting_credit numeric(23,8),
    max_posting_credit numeric(23,8),
    min_posting_debet numeric(23,8),
    max_posting_debet numeric(23,8)
  ) 
  language plpgsql    
as 
$$
  declare
  begin
	return query 
    ------- 
    	
    -- перевод проводок по кредитам в рубли (в ft_postgin_f они в разных валютах)	
	with credit_rub_postings as ( 
      select 
	    (fpf.credit_amount * coalesce(merd.reduced_cource, 1))::numeric as credit_amount_rub
	  from ds.ft_posting_f fpf
        join ds.md_account_d mad 
	      on mad.account_rk = fpf.credit_account_rk 
        left join ds.md_exchange_rate_d merd 
	      on merd.currency_rk = mad.currency_rk 
	        and i_OnDate between merd.data_actual_date and merd.data_actual_end_date
        where 
	      oper_date = i_OnDate 
	      and i_OnDate between mad.data_actual_date and mad.data_actual_end_date
    	    
    -- перевод проводок по дебетам в рубли (в ft_postgin_f они в разных валютах)	      
	), debet_rub_postings as (
      select 
	    (fpf.debet_amount  * coalesce(merd.reduced_cource, 1))::numeric as debet_amount_rub
	  from ds.ft_posting_f fpf
        join ds.md_account_d mad 
	      on mad.account_rk = fpf.debet_account_rk  
        left join ds.md_exchange_rate_d merd 
	      on merd.currency_rk = mad.currency_rk 
	        and i_OnDate between merd.data_actual_date and merd.data_actual_end_date
        where 
	      oper_date = i_OnDate 
	      and i_OnDate between mad.data_actual_date and mad.data_actual_end_date

    )
    -- расчёт показателей (наибольшие и наименьшие проводки за день )   
	  select
	    i_OnDate as date_posting,
	    (select min(credit_amount_rub) from credit_rub_postings) as min_posting_credit,
        (select max(credit_amount_rub) from credit_rub_postings) as max_posting_credit,
  	    (select min(debet_amount_rub) from debet_rub_postings) as min_posting_debet,
        (select max(debet_amount_rub) from debet_rub_postings) as max_posting_debet;
       
    ------    
  end;
$$


