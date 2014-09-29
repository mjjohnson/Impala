-- Note that all of the tests below are duplicated to ensure that the correct
-- results happen no matter which is the left-hand or right-hand table.

-- Test a simple, normal join.
select tab1.col_1, MAX(tab2.col_2), MIN(tab2.col_2) FROM tab2 JOIN tab1 USING (id) GROUP BY col_1 ORDER BY 1 LIMIT 5;
select tab1.col_1, MAX(tab2.col_2), MIN(tab2.col_2) FROM tab1 JOIN tab2 USING (id) GROUP BY col_1 ORDER BY 1 LIMIT 5;
-- What about a join that should be empty?
select MAX(tab2.col_2), MIN(tab2.col_2) FROM tab2 JOIN htest USING (id) GROUP BY col_1 ORDER BY 1 LIMIT 5;
select MAX(tab2.col_2), MIN(tab2.col_2) FROM htest JOIN tab2 USING (id) GROUP BY col_1 ORDER BY 1 LIMIT 5;
-- Nulls?
select * FROM itest JOIN mj_test_table on itest.id=mj_test_table.id ORDER BY 1;
select * FROM itest JOIN mj_test_table on itest.id=mj_test_table.id2 ORDER BY 1;
select * FROM itest JOIN mj_test_table on mj_test_table.id=itest.id ORDER BY 1;
select * FROM itest JOIN mj_test_table on mj_test_table.id2=itest.id ORDER BY 1;
