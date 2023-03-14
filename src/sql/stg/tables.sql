CREATE TABLE IF NOT EXISTS E8ECA156YANDEXBY__STAGING.currencies (
	date_update timestamp NULL,
	currency_code int NULL,
	currency_code_with int NULL,
	currency_with_div numeric(5, 3) NULL
)
order by date_update
SEGMENTED BY hash(date_update::date) all nodes
PARTITION BY date_update::date
GROUP BY calendar_hierarchy_day(date_update::date, 3, 2);

CREATE PROJECTION IF NOT EXISTS currencies_by_dates as 
SELECT * 
FROM E8ECA156YANDEXBY__STAGING.currencies
ORDER BY date_update 
SEGMENTED BY hash(date_update::date) all nodes; 

CREATE TABLE IF NOT EXISTS E8ECA156YANDEXBY__STAGING.transactions (
	operation_id varchar(60) NULL,
	account_number_from int NULL,
	account_number_to int NULL,
	currency_code int NULL,
	country varchar(30) NULL,
	status varchar(30) NULL,
	transaction_type varchar(30) NULL,
	amount int NULL,
	transaction_dt timestamp NULL
)
order by transaction_dt
SEGMENTED BY hash(operation_id, transaction_dt::date) all nodes
PARTITION BY transaction_dt::date
GROUP BY calendar_hierarchy_day(transaction_dt::date, 3, 2);


CREATE PROJECTION IF NOT EXISTS transactions_by_dates as
SELECT *
FROM E8ECA156YANDEXBY__STAGING.transactions
ORDER BY transaction_dt
SEGMENTED BY hash(transaction_dt::date) all nodes;

CREATE TABLE IF NOT EXISTS E8ECA156YANDEXBY__DWH.global_metrics (
	date_update timestamp NOT NULL,
	currency_from varchar NULL,
	cnt_transactions int NULL ,
	amount_total numeric(20, 3) NULL,
	avg_transactions_per_account numeric(5, 3) NULL,
	cnt_accounts_make_transactions int NULL
)
order by date_update
SEGMENTED BY hash(date_update::date) all nodes
PARTITION BY date_update::date
GROUP BY calendar_hierarchy_day(date_update::date, 3, 2);