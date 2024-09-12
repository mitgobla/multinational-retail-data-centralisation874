-- Create a column, char length 14 from Truck_Required
ALTER TABLE public.dim_products
	ADD COLUMN weight_category VARCHAR(14);

UPDATE public.dim_products
SET weight_category = CASE
	WHEN weight < 2 THEN 'Light'
	WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
	WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
	WHEN weight >= 140 THEN 'Truck_Required'
END;