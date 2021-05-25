with total_booked_transaction as
(select * from TRANSACTIONS.FACT_TRANSACTION where transaction_type=1),

total_payment_transaction as
(select * from TRANSACTIONS.FACT_TRANSACTION where transaction_type=2),

total_on_time_payment as (
select count(1) as total from total_booked_transaction as a inner join total_payment_transaction as b on
a.transaction_id=b.transaction_id and a.transaction_date=b.transaction_date and a.total_amount=b.total_amount),

total_transaction as (select count(1) as total from TRANSACTIONS.FACT_TRANSACTION where transaction_type=1)

select (total_on_time_payment.total/total_transaction.total)*100 as percent_transaction_on_time from total_on_time_payment,total_transaction;