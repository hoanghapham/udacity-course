--
-- PostgreSQL database dump
--

-- Dumped from database version 14.5 (Ubuntu 14.5-1.pgdg20.04+1)
-- Dumped by pg_dump version 14.5 (Ubuntu 14.5-1.pgdg20.04+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: artists; Type: TABLE; Schema: public; Owner: student
--

CREATE TABLE public.artists (
    artist_id text NOT NULL,
    name text,
    location text,
    latitude double precision,
    longitude double precision
);


ALTER TABLE public.artists OWNER TO student;

--
-- Name: songplays; Type: TABLE; Schema: public; Owner: student
--

CREATE TABLE public.songplays (
    songplay_id integer NOT NULL,
    start_time timestamp without time zone,
    user_id integer,
    level text,
    song_id text,
    artist_id text,
    session_id integer,
    location text,
    user_agent text
);


ALTER TABLE public.songplays OWNER TO student;

--
-- Name: songs; Type: TABLE; Schema: public; Owner: student
--

CREATE TABLE public.songs (
    song_id text NOT NULL,
    title text,
    artist_id text,
    year integer,
    duration integer
);


ALTER TABLE public.songs OWNER TO student;

--
-- Name: time; Type: TABLE; Schema: public; Owner: student
--

CREATE TABLE public."time" (
    start_time timestamp without time zone,
    hour integer,
    day integer,
    week integer,
    month integer,
    year integer,
    weekday text
);


ALTER TABLE public."time" OWNER TO student;

--
-- Name: users; Type: TABLE; Schema: public; Owner: student
--

CREATE TABLE public.users (
    user_id integer NOT NULL,
    first_name text,
    last_name text,
    gender text,
    level text
);


ALTER TABLE public.users OWNER TO student;

--
-- Name: artists artists_pkey; Type: CONSTRAINT; Schema: public; Owner: student
--

ALTER TABLE ONLY public.artists
    ADD CONSTRAINT artists_pkey PRIMARY KEY (artist_id);


--
-- Name: songplays songplays_pkey; Type: CONSTRAINT; Schema: public; Owner: student
--

ALTER TABLE ONLY public.songplays
    ADD CONSTRAINT songplays_pkey PRIMARY KEY (songplay_id);


--
-- Name: songs songs_pkey; Type: CONSTRAINT; Schema: public; Owner: student
--

ALTER TABLE ONLY public.songs
    ADD CONSTRAINT songs_pkey PRIMARY KEY (song_id);


--
-- Name: users users_pkey; Type: CONSTRAINT; Schema: public; Owner: student
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (user_id);


--
-- PostgreSQL database dump complete
--
