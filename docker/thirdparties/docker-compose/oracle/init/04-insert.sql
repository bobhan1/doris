-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

insert into doris_test.student values (1, 'alice', 20, 99.5);
insert into doris_test.student values (2, 'bob', 21, 90.5);
insert into doris_test.student values (3, 'jerry', 23, 88.0);
insert into doris_test.student values (4, 'andy', 21, 93);

insert into doris_test.test_num values
(1, 111, 123, 7456123.89, 573, 34, 673.43, 34.1264, 56.2, 23.231);

insert into doris_test.test_int values
(1, 99, 9999, 999999999, 999999999999999999, 999, 99999, 9999999999, 9999999999999999999);
insert into doris_test.test_int values
(2, -99, -9999, -999999999, -999999999999999999, -999, -99999, -9999999999, -9999999999999999999);
insert into doris_test.test_int values
(3, 9.9, 99.99, 999999999, 999999999999999999, 999, 99999, 9999999999, 9999999999999999999);


insert into doris_test.test_char values (1, '1', 'china', 'beijing', 'alice', 'abcdefghrjkmnopq');
insert into doris_test.test_char values (2, '2', 'china', 'shanghai', 'bob', 'abcdefghrjkmnopq');
insert into doris_test.test_char values (3, '3', 'Americ', 'new york', 'Jerry', 'abcdefghrjkmnopq');


insert into doris_test.test_raw values (1, hextoraw('ffff'), hextoraw('aaaa'));
insert into doris_test.test_raw values (2, utl_raw.cast_to_raw('beijing'), utl_raw.cast_to_raw('shanghai'));

insert into doris_test.test_date (id, t1) values (1, to_date('2022-1-21 5:23:01','yyyy-mm-dd hh24:mi:ss'));
insert into doris_test.test_date (id, t1) values (2, to_date('20221112203256', 'yyyymmddhh24miss'));
insert into doris_test.test_date (id, t2) values (3, interval '11' year);
insert into doris_test.test_date (id, t2) values (4, interval '223-9' year(3) to month);
insert into doris_test.test_date (id, t3) values (5, interval '12 10:23:01.1234568' day to second);

insert into doris_test.test_timestamp (id, t1) values (1, to_date('2013-1-21 5:23:01','yyyy-mm-dd hh24:mi:ss'));
insert into doris_test.test_timestamp (id, t1) values (2, to_date('20131112203256', 'yyyymmddhh24miss'));
insert into doris_test.test_timestamp (id, t2) values (3, to_timestamp('20191112203357.999', 'yyyymmddhh24miss.ff'));
insert into doris_test.test_timestamp (id, t3) values (4, to_timestamp('20191112203357.999997623', 'yyyymmddhh24miss.ff'));
insert into doris_test.test_timestamp (id, t4) values (5, to_timestamp_tz('20191112203357.999996623', 'yyyymmddhh24miss.ff'));
insert into doris_test.test_timestamp (id, t5) values (6, to_timestamp_tz('20191112203357.999996623', 'yyyymmddhh24miss.ff'));
insert into doris_test.test_timestamp (id, t6) values (7, interval '11' year);
insert into doris_test.test_timestamp (id, t6) values (8, interval '223-9' year(3) to month);
insert into doris_test.test_timestamp (id, t7) values (9, interval '12 10:23:01.1234568' day to second);

insert into doris_test.test_number values (1, 123.45, 12345, 0.0012345);
insert into doris_test.test_number values (2, 123.45, 12345, 0.0099999);
insert into doris_test.test_number values (3, 123.456, 123456.12, 0.00123456);
insert into doris_test.test_number values (4, 12.3456, 1234567, 0.001234567);
insert into doris_test.test_number values (5, 123.56, 9999899, 0.009999899);

insert into doris_test.test_number2 values (1, 12345678901234567890123456789012345678);
insert into doris_test.test_number2 values (2, 99999999999999999999999999999999999999);
insert into doris_test.test_number2 values (3, 999999999999999999999999999999999999999);
insert into doris_test.test_number2 values (4, 12345678);
insert into doris_test.test_number2 values (5, 123.123);
insert into doris_test.test_number2 values (6, 0.999999999999);

insert into doris_test.test_number3 values (1, 9999);
insert into doris_test.test_number3 values (2, 12345678901234567890123456789012345678);
insert into doris_test.test_number3 values (3, 99999999999999999999999999999999999999);
insert into doris_test.test_number3 values (4, 0.99999);

insert into doris_test.test_number4 values (1, 12345678);
insert into doris_test.test_number4 values (2, 123456789012);
insert into doris_test.test_clob values (10086, 'yidong');
insert into doris_test.test_clob values (10010, 'liantong');

insert into doris_test."AA/D" values (1, 'alice', 20, 99.5);
insert into doris_test.aaad values (1, 'alice');

insert into doris_test."student2" values (1, 'alice', 20, 99.5);
insert into doris_test."student2" values (2, 'bob', 21, 90.5);
insert into doris_test."student2" values (3, 'jerry', 23, 88.0);
insert into doris_test."student2" values (4, 'andy', 21, 93);

insert into doris_test."student3" values(1, 'doris', 3, 1.0);

insert into doris_test.test_all_types values
(1, 111, 123, 7456123.89, 573, 34, 673.43, 34.1264, 56.2, 23.231,
99, 9999, 999999999, 999999999999999999, 999, 99999, 9999999999, 9999999999999999999,
'1', 'china', 'beijing', 'alice', 'abcdefghrjkmnopq',
123.45, 12345, 0.0012345,
to_date('2022-1-21 5:23:01','yyyy-mm-dd hh24:mi:ss'), to_timestamp('20191112203357.999', 'yyyymmddhh24miss.ff'), to_timestamp('20191112203357.999997623', 'yyyymmddhh24miss.ff'), to_timestamp_tz('20191112203357.999996623', 'yyyymmddhh24miss.ff'), to_timestamp_tz('20191112203357.999996623', 'yyyymmddhh24miss.ff'), interval '223-9' year(3) to month, interval '12 10:23:01.1234568' day to second 
);
insert into doris_test.test_all_types values
(2, null, null, null, null, null, null, null, null, null,
null, null, null, null, null, null, null, null,
null, null, null, null, null,
null, null, null,
null, null, null, null, null, null, null
);

insert into doris_test.lower_test values ('DORIS', 'Doris', 'doris');

INSERT INTO doris_test.extreme_test VALUES (2147483647, 9999999999999999999999999999999999999, 99999999999999999999999999999999999999, 999999.99, 2147483647, 32767, 999.99, 3.402823466E+38, 3.402823466E+38, 3.402823466E+38, 99, 9999, 999999999, 999999999999999999, 999, 99999, 9999999999, 9999999999999999999, 'Z', '北京', RPAD('A', 4000, 'A'), '深圳', RPAD('L', 4000, 'L'), 999.99, 99900, 0.0000999, TO_DATE('9999-12-31', 'YYYY-MM-DD'), TIMESTAMP '9999-12-31 23:59:59.999', TIMESTAMP '9999-12-31 23:59:59.999999', TIMESTAMP '9999-12-31 23:59:59.999999999', TIMESTAMP '9999-12-31 23:59:59.999999', INTERVAL '99-11' YEAR TO MONTH, INTERVAL '99 23:59:59.999999' DAY TO SECOND);
INSERT INTO doris_test.extreme_test VALUES (-2147483648, -9999999999999999999999999999999999999, -99999999999999999999999999999999999999, -9999999.99, -2147483648, -32768, -999.99, -3.402823466E+38, -3.402823466E+38, -3.402823466E+38, -99, -9999, -999999999, -999999999999999999, -999, -99999, -9999999999, -9999999999999999999, 'A', '上海', 'B', '广州', 'S', -999.99, -99900, -0.0000999, TO_DATE('0001-01-01', 'YYYY-MM-DD'), TIMESTAMP '0001-01-01 00:00:00.000', TIMESTAMP '0001-01-01 00:00:00.000000', TIMESTAMP '0001-01-01 00:00:00.000000000', TIMESTAMP '0001-01-01 00:00:00.000000', INTERVAL '-99-11' YEAR TO MONTH, INTERVAL '-99 23:59:59.999999' DAY TO SECOND);
INSERT INTO doris_test.extreme_test VALUES (0, 0, 0, 0.00, 0, 0, 0.00, 0.0, 0.0, 0.0, 0, 0, 0, 0, 0, 0, 0, 0, ' ', '      ', '', '', '', 0.00, 0, 0.0000000, TO_DATE('1970-01-01', 'YYYY-MM-DD'), TIMESTAMP '1970-01-01 00:00:00.000', TIMESTAMP '1970-01-01 00:00:00.000000', TIMESTAMP '1970-01-01 00:00:00.000000000', TIMESTAMP '1970-01-01 00:00:00.000000', INTERVAL '0-0' YEAR TO MONTH, INTERVAL '0 00:00:00.000000' DAY TO SECOND);
INSERT INTO doris_test.extreme_test VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

INSERT INTO doris_test.extreme_test_multi_block (
    id, n1, n2, n3, n4, n5, n6, n7, n8, n9, 
    tinyint_value1, smallint_value1, int_value1, bigint_value1,
    tinyint_value2, smallint_value2, int_value2, bigint_value2,
    country, city, address, name, remark,
    num1, num2, num4,
    t1, t2, t3, t4, t5, t6, t7
)
SELECT 
    id, n1, n2, n3, n4, n5, n6, n7, n8, n9,
    tinyint_value1, smallint_value1, int_value1, bigint_value1,
    tinyint_value2, smallint_value2, int_value2, bigint_value2,
    country, city, address, name, '',
    num1, num2, num4,
    t1, t2, t3, t4, t5, t6, t7
FROM doris_test.extreme_test;

INSERT INTO doris_test.extreme_test_multi_block (id, n1, n2, n3, n4, n5, n6, n7, n8, n9, tinyint_value1, smallint_value1, int_value1, bigint_value1, tinyint_value2, smallint_value2, int_value2, bigint_value2, country, city, address, name, remark, num1, num2, num4, t1, t2, t3, t4, t5, t6, t7) SELECT id, n1, n2, n3, n4, n5, n6, n7, n8, n9, tinyint_value1, smallint_value1, int_value1, bigint_value1, tinyint_value2, smallint_value2, int_value2, bigint_value2, country, city, address, name, '', num1, num2, num4, t1, t2, t3, t4, t5, t6, t7 FROM doris_test.extreme_test_multi_block;
INSERT INTO doris_test.extreme_test_multi_block (id, n1, n2, n3, n4, n5, n6, n7, n8, n9, tinyint_value1, smallint_value1, int_value1, bigint_value1, tinyint_value2, smallint_value2, int_value2, bigint_value2, country, city, address, name, remark, num1, num2, num4, t1, t2, t3, t4, t5, t6, t7) SELECT id, n1, n2, n3, n4, n5, n6, n7, n8, n9, tinyint_value1, smallint_value1, int_value1, bigint_value1, tinyint_value2, smallint_value2, int_value2, bigint_value2, country, city, address, name, '', num1, num2, num4, t1, t2, t3, t4, t5, t6, t7 FROM doris_test.extreme_test_multi_block;
INSERT INTO doris_test.extreme_test_multi_block (id, n1, n2, n3, n4, n5, n6, n7, n8, n9, tinyint_value1, smallint_value1, int_value1, bigint_value1, tinyint_value2, smallint_value2, int_value2, bigint_value2, country, city, address, name, remark, num1, num2, num4, t1, t2, t3, t4, t5, t6, t7) SELECT id, n1, n2, n3, n4, n5, n6, n7, n8, n9, tinyint_value1, smallint_value1, int_value1, bigint_value1, tinyint_value2, smallint_value2, int_value2, bigint_value2, country, city, address, name, '', num1, num2, num4, t1, t2, t3, t4, t5, t6, t7 FROM doris_test.extreme_test_multi_block;
INSERT INTO doris_test.extreme_test_multi_block (id, n1, n2, n3, n4, n5, n6, n7, n8, n9, tinyint_value1, smallint_value1, int_value1, bigint_value1, tinyint_value2, smallint_value2, int_value2, bigint_value2, country, city, address, name, remark, num1, num2, num4, t1, t2, t3, t4, t5, t6, t7) SELECT id, n1, n2, n3, n4, n5, n6, n7, n8, n9, tinyint_value1, smallint_value1, int_value1, bigint_value1, tinyint_value2, smallint_value2, int_value2, bigint_value2, country, city, address, name, '', num1, num2, num4, t1, t2, t3, t4, t5, t6, t7 FROM doris_test.extreme_test_multi_block;
INSERT INTO doris_test.extreme_test_multi_block (id, n1, n2, n3, n4, n5, n6, n7, n8, n9, tinyint_value1, smallint_value1, int_value1, bigint_value1, tinyint_value2, smallint_value2, int_value2, bigint_value2, country, city, address, name, remark, num1, num2, num4, t1, t2, t3, t4, t5, t6, t7) SELECT id, n1, n2, n3, n4, n5, n6, n7, n8, n9, tinyint_value1, smallint_value1, int_value1, bigint_value1, tinyint_value2, smallint_value2, int_value2, bigint_value2, country, city, address, name, '', num1, num2, num4, t1, t2, t3, t4, t5, t6, t7 FROM doris_test.extreme_test_multi_block;
INSERT INTO doris_test.extreme_test_multi_block (id, n1, n2, n3, n4, n5, n6, n7, n8, n9, tinyint_value1, smallint_value1, int_value1, bigint_value1, tinyint_value2, smallint_value2, int_value2, bigint_value2, country, city, address, name, remark, num1, num2, num4, t1, t2, t3, t4, t5, t6, t7) SELECT id, n1, n2, n3, n4, n5, n6, n7, n8, n9, tinyint_value1, smallint_value1, int_value1, bigint_value1, tinyint_value2, smallint_value2, int_value2, bigint_value2, country, city, address, name, '', num1, num2, num4, t1, t2, t3, t4, t5, t6, t7 FROM doris_test.extreme_test_multi_block;
INSERT INTO doris_test.extreme_test_multi_block (id, n1, n2, n3, n4, n5, n6, n7, n8, n9, tinyint_value1, smallint_value1, int_value1, bigint_value1, tinyint_value2, smallint_value2, int_value2, bigint_value2, country, city, address, name, remark, num1, num2, num4, t1, t2, t3, t4, t5, t6, t7) SELECT id, n1, n2, n3, n4, n5, n6, n7, n8, n9, tinyint_value1, smallint_value1, int_value1, bigint_value1, tinyint_value2, smallint_value2, int_value2, bigint_value2, country, city, address, name, '', num1, num2, num4, t1, t2, t3, t4, t5, t6, t7 FROM doris_test.extreme_test_multi_block;
INSERT INTO doris_test.extreme_test_multi_block (id, n1, n2, n3, n4, n5, n6, n7, n8, n9, tinyint_value1, smallint_value1, int_value1, bigint_value1, tinyint_value2, smallint_value2, int_value2, bigint_value2, country, city, address, name, remark, num1, num2, num4, t1, t2, t3, t4, t5, t6, t7) SELECT id, n1, n2, n3, n4, n5, n6, n7, n8, n9, tinyint_value1, smallint_value1, int_value1, bigint_value1, tinyint_value2, smallint_value2, int_value2, bigint_value2, country, city, address, name, '', num1, num2, num4, t1, t2, t3, t4, t5, t6, t7 FROM doris_test.extreme_test_multi_block;
INSERT INTO doris_test.extreme_test_multi_block (id, n1, n2, n3, n4, n5, n6, n7, n8, n9, tinyint_value1, smallint_value1, int_value1, bigint_value1, tinyint_value2, smallint_value2, int_value2, bigint_value2, country, city, address, name, remark, num1, num2, num4, t1, t2, t3, t4, t5, t6, t7) SELECT id, n1, n2, n3, n4, n5, n6, n7, n8, n9, tinyint_value1, smallint_value1, int_value1, bigint_value1, tinyint_value2, smallint_value2, int_value2, bigint_value2, country, city, address, name, '', num1, num2, num4, t1, t2, t3, t4, t5, t6, t7 FROM doris_test.extreme_test_multi_block;
INSERT INTO doris_test.extreme_test_multi_block (id, n1, n2, n3, n4, n5, n6, n7, n8, n9, tinyint_value1, smallint_value1, int_value1, bigint_value1, tinyint_value2, smallint_value2, int_value2, bigint_value2, country, city, address, name, remark, num1, num2, num4, t1, t2, t3, t4, t5, t6, t7) SELECT id, n1, n2, n3, n4, n5, n6, n7, n8, n9, tinyint_value1, smallint_value1, int_value1, bigint_value1, tinyint_value2, smallint_value2, int_value2, bigint_value2, country, city, address, name, '', num1, num2, num4, t1, t2, t3, t4, t5, t6, t7 FROM doris_test.extreme_test_multi_block;

INSERT INTO doris_test.extreme_test_multi_block (
    id, n1, n2, n3, n4, n5, n6, n7, n8, n9, 
    tinyint_value1, smallint_value1, int_value1, bigint_value1,
    tinyint_value2, smallint_value2, int_value2, bigint_value2,
    country, city, address, name, remark,
    num1, num2, num4,
    t1, t2, t3, t4, t5, t6, t7
)
SELECT 
    id, n1, n2, n3, n4, n5, n6, n7, n8, n9,
    tinyint_value1, smallint_value1, int_value1, bigint_value1,
    tinyint_value2, smallint_value2, int_value2, bigint_value2,
    country, city, address, name, '',
    num1, num2, num4,
    t1, t2, t3, t4, t5, t6, t7
FROM doris_test.extreme_test;

commit;
