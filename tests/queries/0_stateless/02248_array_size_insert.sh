#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

EXCEPTION_SUCCESS_TEXT="Error handled"

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS test_array_size_insert;"

$CLICKHOUSE_CLIENT --query="CREATE TABLE test_array_size_insert
(
        a       Array(UInt8, 3),
        b       Array(UInt8, 1)
)
ENGINE = MergeTree ORDER BY tuple();"

EXCEPTION_TEXT="Size of input array"

# Must throw an exception
$CLICKHOUSE_CLIENT --query="insert into test_array_size_insert VALUES (array(1,2), array(1,2));" 2>&1 \
    | grep -q "$EXCEPTION_TEXT" && echo "$EXCEPTION_SUCCESS_TEXT" || echo "Did not thrown an exception"
$CLICKHOUSE_CLIENT --query="insert into test_array_size_insert VALUES ([1,2], [1,2]);" 2>&1 \
    | grep -q "$EXCEPTION_TEXT" && echo "$EXCEPTION_SUCCESS_TEXT" || echo "Did not thrown an exception"

$CLICKHOUSE_CLIENT --query="insert into test_array_size_insert VALUES (array(1,2,3), array(1,2));" 2>&1 \
    | grep -q "$EXCEPTION_TEXT" && echo "$EXCEPTION_SUCCESS_TEXT" || echo "Did not thrown an exception"
$CLICKHOUSE_CLIENT --query="insert into test_array_size_insert VALUES ([1,2,3], [1,2]);" 2>&1 \
    | grep -q "$EXCEPTION_TEXT" && echo "$EXCEPTION_SUCCESS_TEXT" || echo "Did not thrown an exception"

$CLICKHOUSE_CLIENT --query="insert into test_array_size_insert VALUES (array(1,2), array(1));" 2>&1 \
    | grep -q "$EXCEPTION_TEXT" && echo "$EXCEPTION_SUCCESS_TEXT" || echo "Did not thrown an exception"
$CLICKHOUSE_CLIENT --query="insert into test_array_size_insert VALUES ([1,2], [1]);" 2>&1 \
    | grep -q "$EXCEPTION_TEXT" && echo "$EXCEPTION_SUCCESS_TEXT" || echo "Did not thrown an exception"


# Must not throw an exception
$CLICKHOUSE_CLIENT --query="insert into test_array_size_insert VALUES (array(1,2,3), array(1));"
$CLICKHOUSE_CLIENT --query="insert into test_array_size_insert VALUES ([1,2,3], [1]);"


# Must all throw an exception
$CLICKHOUSE_CLIENT --query="insert into test_array_size_insert VALUES (array(1,2), array(1,2));" 2>&1 \
    | grep -q "$EXCEPTION_TEXT" && echo "$EXCEPTION_SUCCESS_TEXT" || echo "Did not thrown an exception"
$CLICKHOUSE_CLIENT --query="insert into test_array_size_insert VALUES ([1,2], [1,2]);" 2>&1 \
    | grep -q "$EXCEPTION_TEXT" && echo "$EXCEPTION_SUCCESS_TEXT" || echo "Did not thrown an exception"

$CLICKHOUSE_CLIENT --query="insert into test_array_size_insert VALUES (array(1,2,3), array(1,2));" 2>&1 \
    | grep -q "$EXCEPTION_TEXT" && echo "$EXCEPTION_SUCCESS_TEXT" || echo "Did not thrown an exception"
$CLICKHOUSE_CLIENT --query="insert into test_array_size_insert VALUES ([1,2,3], [1,2]);" 2>&1 \
    | grep -q "$EXCEPTION_TEXT" && echo "$EXCEPTION_SUCCESS_TEXT" || echo "Did not thrown an exception"

$CLICKHOUSE_CLIENT --query="insert into test_array_size_insert VALUES (array(1,2), array(1));" 2>&1 \
    | grep -q "$EXCEPTION_TEXT" && echo "$EXCEPTION_SUCCESS_TEXT" || echo "Did not thrown an exception"
$CLICKHOUSE_CLIENT --query="insert into test_array_size_insert VALUES ([1,2], [1]);" 2>&1 \
    | grep -q "$EXCEPTION_TEXT" && echo "$EXCEPTION_SUCCESS_TEXT" || echo "Did not thrown an exception"

$CLICKHOUSE_CLIENT --query="insert into test_array_size_insert VALUES (array(), array());" 2>&1 \
    | grep -q "$EXCEPTION_TEXT" && echo "$EXCEPTION_SUCCESS_TEXT" || echo "Did not thrown an exception"
$CLICKHOUSE_CLIENT --query="insert into test_array_size_insert VALUES ([], []);" 2>&1 \
    | grep -q "$EXCEPTION_TEXT" && echo "$EXCEPTION_SUCCESS_TEXT" || echo "Did not thrown an exception"


# Must not throw an exception
$CLICKHOUSE_CLIENT --query="insert into test_array_size_insert VALUES (array(4,5,6), array(2));"
$CLICKHOUSE_CLIENT --query="insert into test_array_size_insert VALUES ([4,5,6], [2]);"


$CLICKHOUSE_CLIENT --query="select * from test_array_size_insert order by a;"
$CLICKHOUSE_CLIENT --query="DROP TABLE test_array_size_insert;"
