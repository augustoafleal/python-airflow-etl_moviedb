CREATE TABLE IF NOT EXISTS public.movies_rank
(
    rank integer NOT NULL,
    id integer,
    title text COLLATE pg_catalog."default",
    original_language text COLLATE pg_catalog."default",
    overview text COLLATE pg_catalog."default",
    poster text COLLATE pg_catalog."default",
    backdrop text COLLATE pg_catalog."default",
    popularity numeric,
    vote_average numeric,
    vote_count integer,
    created_date timestamp without time zone,
    CONSTRAINT movies_rank_pkey PRIMARY KEY (rank)
)