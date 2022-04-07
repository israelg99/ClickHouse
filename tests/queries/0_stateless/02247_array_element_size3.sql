DROP TABLE IF EXISTS array_element_test;
CREATE TABLE array_element_test (arr Array(Int32, 3), id Int32) ENGINE = Memory;
insert into array_element_test VALUES ([11,12,13], 2), ([11,12,13], 4), ([11,12,13], -1), ([11,12,13], -2), ([11,12,13], -3), ([11,12,13], 0);
select arr[id] from array_element_test;

DROP TABLE IF EXISTS array_element_test;
CREATE TABLE array_element_test (arr Array(Int32, 3), id UInt32) ENGINE = Memory;
insert into array_element_test VALUES ([11,12,13], 2), ([11,12,13], 4), ([11,12,13], 1), ([11,12,13], 4), ([11,12,13], 0);
select arr[id] from array_element_test;

DROP TABLE IF EXISTS array_element_test;
CREATE TABLE array_element_test (arr Array(String, 3), id Int32) ENGINE = Memory;
insert into array_element_test VALUES (['Abc','Df','Q'], 2), (['Abc','DEFQ', 'aiJK'], 4), (['ABC','Q','ERT'], -1), (['Ab','ber','tKjs'], -2), (['AB','asd','tlQf'], -3), (['A','B','C'], 0);
select arr[id] from array_element_test;

DROP TABLE IF EXISTS array_element_test;
CREATE TABLE array_element_test (arr Array(String, 3), id UInt32) ENGINE = Memory;
insert into array_element_test VALUES (['Abc','Df','Q'], 2), (['Abc','DEFQ', 'aiJK'], 4), (['ABC','Q','ERT'], 1), (['Ab','ber','tKjs'], 4), (['A','B','C'], 0);
select arr[id] from array_element_test;
