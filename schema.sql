drop table if exists users;
create table users (
    ip text primary key not null,
    country_code text
);
