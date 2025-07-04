-- Create the CDC source table for Kafka data
CREATE TABLE IF NOT EXISTS `default`.`cdc.test.accounts` (
    kafkaKey String,
    account_id UInt64,
    limit Int64,
    products Array(String),
    __op String,
    __ts_ms UInt64
) ENGINE = MergeTree
ORDER BY __ts_ms;

-- Create the target accounts table with ReplacingMergeTree
CREATE TABLE IF NOT EXISTS default.accounts (
    `_id` String,
    `account_id` UInt64,
    `limit` Int64,
    `products` Array(String),
    `ts_ms` UInt64,
    `is_deleted` UInt8
) ENGINE = ReplacingMergeTree()
ORDER BY _id;

-- Create the materialized view to transform CDC data
CREATE MATERIALIZED VIEW IF NOT EXISTS default.accounts_mv_2 TO default.accounts
AS SELECT
    kafkaKey AS _id,
    account_id,
    limit,
    products,
    __ts_ms AS ts_ms,
    if(__op = 'd', 1, 0) AS is_deleted
FROM default.`cdc.test.accounts`;
