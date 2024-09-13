ALTER TABLE public.dim_products
	RENAME COLUMN removed TO still_available;

-- Flip boolean to match new name
UPDATE public.dim_products
	SET still_available = NOT still_available;

ALTER TABLE public.dim_products
    ALTER COLUMN product_price TYPE FLOAT;

ALTER TABLE public.dim_products
    ALTER COLUMN weight TYPE FLOAT;

-- Longest EAN is 17 digits
-- Loaded as double precision, which shows scientific notation which makes length longer than 17
-- So cast to BIGINT first to avoid notation
ALTER TABLE public.dim_products
    ALTER COLUMN "EAN" TYPE VARCHAR(17) USING "EAN"::BIGINT;

ALTER TABLE public.dim_products
    ALTER COLUMN product_code TYPE VARCHAR(11);

ALTER TABLE public.dim_products
    ALTER COLUMN "uuid" TYPE UUID USING "uuid"::UUID;
