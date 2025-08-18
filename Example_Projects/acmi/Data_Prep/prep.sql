
drop schema if exists acme_edw_dev.oltp cascade;
drop schema if exists acme_edw_dev.edw_raw cascade;
drop schema if exists acme_edw_dev.edw_old cascade;
create catalog if not exists acme_edw_dev;
create schema if not exists acme_edw_dev.edw_raw;
create schema if not exists acme_edw_dev.edw_bronze;
create schema if not exists acme_edw_dev.edw_silver;
create schema if not exists acme_edw_dev.edw_gold;
create schema if not exists acme_edw_dev.edw_old;
create schema if not exists acme_edw_dev.oltp;
