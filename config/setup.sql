-- Create database
CREATE DATABASE IF NOT EXISTS optum;

-- Create schema
CREATE SCHEMA IF NOT EXISTS optum.uhc_tic;

-- Create table
CREATE TABLE IF NOT EXISTS optum.uhc_tic.index_files (
    source_file                 VARCHAR         NOT NULL,
    reporting_date              DATE            NOT NULL,
    processed_at                TIMESTAMP       NOT NULL,
    reporting_entity_name       VARCHAR         NOT NULL,
    reporting_entity_type       VARCHAR         NOT NULL,
    last_updated_on             DATE            NOT NULL,
    version                     VARCHAR         NOT NULL,
    plan_name                   VARCHAR         NOT NULL,
    plan_id                     VARCHAR         NOT NULL,
    plan_id_type                VARCHAR         NOT NULL,
    plan_market_type            VARCHAR         NOT NULL,
    plan_sponsor_name           VARCHAR,
    issuer_name                 VARCHAR         NOT NULL,
    in_network_description      VARCHAR,
    in_network_location         VARCHAR,
    allowed_amount_description  VARCHAR,
    allowed_amount_location     VARCHAR,
    loaded_at                   TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
) CLUSTER BY (reporting_date)
COMMENT = 'UHC Transparency in Coverage — flattened index files. Source: data/processed/uhc_tic/index/';