ALTER TABLE public.dim_card_details
    ALTER COLUMN card_number TYPE VARCHAR(19);

ALTER TABLE public.dim_card_details
	ALTER COLUMN expiry_date TYPE DATE;

ALTER TABLE public.dim_card_details
	ALTER COLUMN date_payment_confirmed TYPE DATE;

-- This column seemed to persist even after deleting it in Pandas
ALTER TABLE public.dim_card_details
	DROP COLUMN level_0;