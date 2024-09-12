ALTER TABLE public.dim_date_times
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;
