create table if not exists main
(
    id        varchar primary key,
    submitter varchar,
    title     varchar,
    doi       varchar,
    abstract  varchar
);

CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS "pgcrypto";


CREATE TEMPORARY TABLE temp_data
(
    data jsonb
);


COPY temp_data (data)
    FROM '/Users/mariojacoboriosgamboa/Lordmarcusvane/Hackprog/universidad/bd2/bd2_project_2/backend/data/processed_data.json';

create index if not exists gin_abstract_idx on main using gin (abstract gin_trgm_ops);

INSERT INTO main (id, submitter, title, doi, abstract)
SELECT data ->> 'id',
    data ->> 'submitter',
    data ->> 'title',
    data ->> 'doi',
    data ->> 'abstract'
FROM temp_data;

drop table temp_data;

