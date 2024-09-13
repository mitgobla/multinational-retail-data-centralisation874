-- Get maximum length of value in a column
SELECT
    LENGTH(store_code) AS max_length
FROM public.orders_table
WHERE
    length(store_code) = (
        SELECT
            MAX(LENGTH(store_code))
        FROM public.orders_table
        )
GROUP BY max_length;

-- Alter orders_table table column types
ALTER TABLE public.orders_table
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;

ALTER TABLE public.orders_table
	ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;

ALTER TABLE public.orders_table
	ALTER COLUMN card_number TYPE VARCHAR(19);

ALTER TABLE public.orders_table
	ALTER COLUMN store_code TYPE VARCHAR(12);

ALTER TABLE public.orders_table
	ALTER COLUMN product_code TYPE VARCHAR(11);

ALTER TABLE public.orders_table
	ALTER COLUMN product_quantity TYPE SMALLINT;
