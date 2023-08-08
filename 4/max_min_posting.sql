select * from ds.ft_posting_f fpf ;
        
create or replace function ds.min_max_posting (i_OnDate  date)
  returns table (
    i_ondate date,
    min_posting_credit numeric,
    max_posting_credit numeric,
    min_posting_debet numeric,
    max_posting_debet numeric,
  ) 
  language plpgsql    
as 
$$
  declare
  begin
	return query 
    ------------
	select
	  i_OnDate,
	  min(credit_amount) as min_posting_credit,
	  max(credit_amount) as max_posting_credit,
	  min(debet_amount) as min_posting_debet,
	  max(debet_amount) as max
	from ds.ft_posting_f
	where 
	  oper_date = i_OnDate
	------------
  end;
$$
