ALTER TABLE public.dim_store_details
    ALTER COLUMN locality TYPE VARCHAR(255);

ALTER TABLE public.dim_store_details
    ALTER COLUMN store_code TYPE VARCHAR(11);

ALTER TABLE public.dim_store_details
    ALTER COLUMN staff_numbers TYPE SMALLINT;

ALTER TABLE public.dim_store_details
    ALTER COLUMN locality TYPE VARCHAR(255);

ALTER TABLE public.dim_store_details
    ALTER COLUMN store_type TYPE VARCHAR(255);

-- Make column nullable
ALTER TABLE public.dim_store_details
    ALTER COLUMN store_type DROP NOT NULL;

ALTER TABLE public.dim_store_details
    ALTER COLUMN country_code TYPE VARCHAR(2);

ALTER TABLE public.dim_store_details
    ALTER COLUMN continent TYPE VARCHAR(255);