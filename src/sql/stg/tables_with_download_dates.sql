CREATE TABLE IF NOT EXISTS E8ECA156YANDEXBY__STAGING.currencies_date (
	date_update timestamp NULL,
	currency_code int NULL,
	currency_code_with int NULL,
	currency_with_div numeric(5, 3) NULL,
	date_download timestamp,
	UNIQUE (date_update, currency_code, currency_code_with, currency_with_div)
)
order by date_update
SEGMENTED BY hash(date_update::date) all nodes
PARTITION BY date_update::date
GROUP BY calendar_hierarchy_day(date_update::date, 3, 2);

INSERT INTO E8ECA156YANDEXBY__STAGING.currencies_date
SELECT DISTINCT *, now() FROM E8ECA156YANDEXBY__STAGING.currencies;

CREATE TABLE IF NOT EXISTS E8ECA156YANDEXBY__STAGING.transactions_date (
	operation_id varchar(60) NULL,
	account_number_from int NULL,
	account_number_to int NULL,
	currency_code int NULL,
	country varchar(30) NULL,
	status varchar(30) NULL,
	transaction_type varchar(30) NULL,
	amount int NULL,
	transaction_dt timestamp NULL,
	date_download timestamp,
	UNIQUE (operation_id)
)
order by transaction_dt
SEGMENTED BY hash(operation_id, transaction_dt::date) all nodes
PARTITION BY transaction_dt::date
GROUP BY calendar_hierarchy_day(transaction_dt::date, 3, 2);

INSERT INTO E8ECA156YANDEXBY__STAGING.transactions_date
SELECT DISTINCT *, now() FROM E8ECA156YANDEXBY__STAGING.transactions;