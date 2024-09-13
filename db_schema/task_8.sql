-- Date events primary key
ALTER TABLE IF EXISTS public.dim_date_times
    ADD CONSTRAINT dim_date_times_pk PRIMARY KEY (date_uuid);

-- Card details primary key
ALTER TABLE IF EXISTS public.dim_card_details
    ADD CONSTRAINT dim_card_details_pk PRIMARY KEY (card_number);

-- Product details primary key
ALTER TABLE IF EXISTS public.dim_products
    ADD CONSTRAINT dim_products_pk PRIMARY KEY (product_code);

-- Store details primary key
ALTER TABLE IF EXISTS public.dim_store_details
    ADD CONSTRAINT dim_store_details_pk PRIMARY KEY (store_code);

-- User details primary key
ALTER TABLE IF EXISTS public.dim_users
    ADD CONSTRAINT dim_users_pk PRIMARY KEY (user_uuid);