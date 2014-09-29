-- Note that all of the tests below are duplicated to ensure that the correct
-- results happen no matter which is the left-hand or right-hand table.


-- Test a simple, normal join.
select tab1.col_1, MAX(tab2.col_2), MIN(tab2.col_2) FROM tab2 JOIN tab1 USING (id) GROUP BY col_1 ORDER BY 1 LIMIT 5;
select tab1.col_1, MAX(tab2.col_2), MIN(tab2.col_2) FROM tab1 JOIN tab2 USING (id) GROUP BY col_1 ORDER BY 1 LIMIT 5;

-- What about a join that should be empty?
select MAX(tab2.col_2), MIN(tab2.col_2) FROM tab2 JOIN htest USING (id) GROUP BY col_1 ORDER BY 1 LIMIT 5;
select MAX(tab2.col_2), MIN(tab2.col_2) FROM htest JOIN tab2 USING (id) GROUP BY col_1 ORDER BY 1 LIMIT 5;

-- Nulls?
select itest.id, mj_test_table.id FROM itest JOIN mj_test_table on itest.id=mj_test_table.id ORDER BY 1;
select itest.id, mj_test_table.id2 FROM mj_test_table JOIN itest on itest.id=mj_test_table.id2 ORDER BY 1;
select itest.id, mj_test_table.id FROM itest JOIN mj_test_table on mj_test_table.id=itest.id ORDER BY 1;
select itest.id, mj_test_table.id2 FROM mj_test_table JOIN itest on mj_test_table.id2=itest.id ORDER BY 1;
select htest.id, mj_test_table.id FROM htest JOIN mj_test_table on htest.id=mj_test_table.id ORDER BY 1;
select htest.id, mj_test_table.id2 FROM mj_test_table JOIN htest on htest.id=mj_test_table.id2 ORDER BY 1;
select htest.id, mj_test_table.id FROM htest JOIN mj_test_table on mj_test_table.id=htest.id ORDER BY 1;
select htest.id, mj_test_table.id2 FROM mj_test_table JOIN htest on mj_test_table.id2=htest.id ORDER BY 1;

-- Nulls in join condition?
select mj_test_table.id, mj_test_table.id2 FROM mj_test_table JOIN mj_test_table mj2 on mj_test_table.id=mj2.id2 ORDER BY 1;
select mj_test_table.id, mj_test_table.id2 FROM mj_test_table mj2 JOIN mj_test_table on mj2.id2=mj_test_table.id ORDER BY 1;

select mj_test_table.id, mj_test_table.id2 FROM mj_test_table JOIN mj_test_table mj2 on mj_test_table.id2=mj2.id2 ORDER BY 1;
select mj_test_table.id, mj_test_table.id2 FROM mj_test_table mj2 JOIN mj_test_table on mj2.id2=mj_test_table.id2 ORDER BY 1;

