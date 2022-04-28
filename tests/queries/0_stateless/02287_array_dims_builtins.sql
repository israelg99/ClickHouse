CREATE TABLE test
(
    a Array(UInt8, 3)
)
    ENGINE = MergeTree ORDER BY tuple();

CREATE TABLE inpt
(
    a Array(UInt8)
)
    ENGINE = MergeTree ORDER BY tuple();
insert into inpt values([1,2,3]);

-- "So yeah just need to store one scalar with column metadata and that's it.
--         I cannot believe their column metadata is so fucked up
--            it takes 6 month to add one scalar parameter."

-- Yes that was exactly the plan and expectation.
-- Here is the results of adding one "dims" scalar parameter to the Array column and type.
-- And updating it in all obvious places.

insert into test values(array(1,2,3)) -- works
insert into test values(array(1,2,3,4)) -- fails "Size doesn't match"

--- But now we have many issues.
--- Most issues are related to inserts.

--- Most culprits are because CH recreates the ArrayColumn many times in places
--              where it is not trivial to know what the target column dims is.

--- Another culprit is because Array behaves like hybrid type
--          can be fixed-size, or dynamic-size
--          so when it operates on columns in runtime
--          it is not trivial to know which state a given column is.

--- And there are other random issues.

insert into test values([1,2,3]); -- fails because it executes different code than "array()"

insert into test select [1,2,3]; -- fails because it reconstructs the ArrayColumn
-- select [1,2,3]; -- ColumnArray(Int, dims=0)
--     it is not trivial to know what the target column dims is so we set it to 0.
--     if arrays were ALWAYS fixed-size then we'll always set it to 3.
--     but since arrays are dynamic size by default we set it to 0.

-- test.a -- ColumnArray(Int, dims=3)

insert into test select a from inpt; -- fails for same reason
-- test.a -- ColumnArray(Int, dims=3)
-- inpt.a -- ColumnArray(Int, dims=0)
-- this is bad because if user wants to try new feature
--      they can't even load existing array column

insert into test select a from test; -- works since it is same column.

insert into test select arraySort(a) from test; -- fails since builtins recreate ArrayColumn
--                                                    and it is not trivial to know what the target column dims is.
--                                                    so we set it to 0.
-- select arraySort(a) = ColumnArray(Int, dims=0)

-- same for all builtins - arrayReverse, arrayCumSum, arraySort,
--                         arraySlice, arrayPush, arrayPop, arrayResize, etc...

-- Functions fail too for the same reason:
CREATE FUNCTION get_array as () -> array([1,2,3]);
insert into test select get_array(); -- fails for same reason

-- Alter table is broken.
ALTER TABLE test MODIFY COLUMN a Array(UInt8, 2);
select a from test;
-- [1,2,3]
describe table test;
-- a Array(UInt8, 2)


-- alter works but data is not consistent
-- no easy solution





-- Create tables, views, and materialized views
DROP TABLE IF EXISTS test;
CREATE TABLE test
(
    a Array(UInt8, 3),
    b Array(UInt8, 3)
)
    ENGINE = MergeTree ORDER BY tuple();

DROP TABLE IF EXISTS inpt;
CREATE TABLE inpt
(
    a Array(UInt8, 3),
    b Array(UInt8, 3)
)
    ENGINE = MergeTree ORDER BY tuple();

DROP TABLE IF EXISTS mv_table;
CREATE MATERIALIZED VIEW mv_table
ENGINE = MergeTree ORDER BY a
AS SELECT
  a,
  b
FROM test;

DROP TABLE IF EXISTS v_table;
CREATE VIEW v_table AS SELECT * FROM mv_table;

-- Create functions
DROP FUNCTION IF EXISTS array_builtins;
CREATE FUNCTION array_builtins as (a) -> arraySort(arrayPopFront(arrayReverse(arrayPushBack(arrayCumSum(a), 1))));

DROP FUNCTION IF EXISTS get_array;
CREATE FUNCTION get_array as () -> array([1,2,3]);

-- Input data
INSERT INTO inpt VALUES ([64,29,98], [22,101,81]);
INSERT INTO inpt VALUES ([74,38,55], [92,29,47]);

-- Inserts
INSERT INTO test SELECT arraySort(a), arrayReverseSort(b) from inpt;
INSERT INTO test SELECT arraySort(x -> -x, b), arrayReverseSort(x -> -x, a) from inpt;
INSERT INTO test SELECT arrayCumSum(a), arrayCumSumNonNegative(b) from inpt;
INSERT INTO test SELECT arrayReverse(a), arrayReverseSort(b) from inpt;
INSERT INTO test SELECT arrayPopFront(arrayPushFront(a, 1)), arrayPushBack(arrayPopBack(b), 1) from inpt;
INSERT INTO test SELECT arrayCumSum(b), arrayCumSumNonNegative(a) from inpt;



-- Probably due to sinks/sources/sliceBounded/sliceUnbounded
INSERT INTO test SELECT arraySlice(a, 1, 3), arraySlice(b, -1, 3) from inpt;
INSERT INTO test SELECT arraySlice(arrayCumSum(arrayPushFront(a, 12)), 1, 3), arraySlice(arrayCumSumNonNegative(arrayPushBack(b, 82)), -1, 3) from inpt;

-- Fails because we need to fix data type probably
INSERT INTO test SELECT arraySort(arrayPopFront(arrayReverse(arrayPushBack(arrayCumSum(a), 1)))), arraySort(arrayPopFront(arrayReverse(arrayPushBack(arrayCumSum(b), 1)))) from inpt;
INSERT INTO test SELECT array_builtins(a), array_builtins(b) from inpt;
INSERT INTO test SELECT array_builtins(b), array_builtins(a) from inpt;

-- Needs to be fixed
INSERT INTO test SELECT arrayResize(a, 1), arrayResize(b, 1) from inpt;


-- Works - test for expected failure
INSERT INTO test SELECT arrayPopFront(arrayPopFront(a)), arrayPopFront(arrayPopFront(b)) from inpt;


-- Join table test with materialized view
INSERT INTO test SELECT a, mv_table.b from test JOIN mv_table ON test.a = mv_table.a WHERE mv_table.a[1] = 64;

-- Insert from view and materialized view
INSERT INTO test SELECT arraySort(a), arrayReverseSort(b) from v_table;
INSERT INTO test SELECT arrayCumSum(a), arrayCumSumNonNegative(b) from mv_table;

-- Select from table, view, and materialized view
SELECT * FROM v_table;
SELECT * from mv_table;
SELECT * FROM test;

-- Those are expected to fail since we don't know the dims of the array.
-- A `vec` builtin will solve those use cases.
-- INSERT INTO test SELECT arraySort([11,5,63]), arrayReverseSort([12,53,21]);
-- INSERT INTO test SELECT arraySort(x -> -x, [52,48,122]), arrayReverseSort(x -> -x, [83,49,4]);
--
-- INSERT INTO test SELECT arrayCumSum([1,2,3]), arrayCumSumNonNegative([3,2,1]);
-- INSERT INTO test SELECT arrayReverse([11,22,33]), arrayReverseSort([53,12,66]);
-- INSERT INTO test SELECT arrayPopFront(arrayPushFront([1,2,3], 1)), arrayPushBack(arrayPopBack([3,2,1]), 1);

-- This is probably expected failure
INSERT INTO test SELECT get_array(), get_array();
