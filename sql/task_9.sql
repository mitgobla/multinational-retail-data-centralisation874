ALTER TABLE public.orders_table
	-- Date table foreign key
	ADD CONSTRAINT fk_date_uuid
	FOREIGN KEY (date_uuid) REFERENCES public.dim_date_times (date_uuid),

	-- User table foreign key
	ADD CONSTRAINT fk_user_uuid
	FOREIGN KEY (user_uuid) REFERENCES public.dim_users (user_uuid),

	-- Card table foreign key
	ADD CONSTRAINT fk_card_number
	FOREIGN KEY (card_number) REFERENCES public.dim_card_details (card_number),

	-- Store table foreign key
	ADD CONSTRAINT fk_store_code
	FOREIGN KEY (store_code) REFERENCES public.dim_store_details (store_code),

	-- Product table foreign key
	ADD CONSTRAINT fk_product_code
	FOREIGN KEY (product_code) REFERENCES public.dim_products (product_code);