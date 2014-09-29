#!/usr/bin/env python

import os
from subprocess import Popen, PIPE
import sys
import time


def run_query(query, enable_custom_op):
    """Run an Impala query and return the results.

    Args:
        query: A string containing Impala query to execute.
        enable_custom_op: A boolean value, denoting whether to set
            enable_custom_op to true or false.

    Returns:
        A string containing stdout from Impala, as it would appear if the
            given query were executed in the Impala shell.
    """
    p = Popen([os.environ["IMPALA_HOME"] + "/bin/impala-shell.sh"], stdin=PIPE,
            stdout=PIPE, stderr=PIPE)
    input = "set enable_custom_op={custom}; {query}".format(
            custom='true' if enable_custom_op else 'false', query=query)
    output = p.communicate(input=input)
    if 'ERROR' in output[0] or 'ERROR' in output[1]:
        raise Exception("the following query: {query} returned an error: "
                "{error}".format(query=query, error=output))
    return output[0]


def strip_output(output):
    """Remove extraneous lines from Impala's output.

    The first several lines and the last just clutter up the results returned
    by the query, so this removes them.

    Args:
        output: A string containing Impala output.

    Returns:
        A version of the output showing only the query results returned.
    """
    output_lines = output.splitlines()[6:-1]
    return '\n'.join(output_lines)


def create_test_table():
    """Create a table with additional test data."""
    sql = """DROP TABLE IF EXISTS mj_test_table;
             CREATE TABLE mj_test_table (id INT, id2 INT) STORED AS TEXTFILE;
             INSERT INTO mj_test_table values
               (1,NULL), (137,5000), (NULL,5000), (5000,NULL);"""
    run_query(sql, False)


def run_tests(filename):
    """Run the Impala tests contained in filename.

    Args:
        filename: A string containing the name of a file that has SQL
            statements to be run on Impala and compared with and without the
            custom block nested join enabled.
    """
    create_test_table()
    tests = 0
    failures = 0
    # Time how long the tests take to run
    start = time.time()
    with open(filename, 'rU') as f:
        for line in f:
            line = line.strip()
            # Ignore blank lines and treat -- as commented lines.
            if not line or line.startswith('--'):
                continue

            tests += 1
            orig_output = run_query(line, False)
            bnl_output = run_query(line, True)

            # The two outputs should be equal if the custom join is correct
            if orig_output != bnl_output:
                failures += 1
                print "got different output for query: {query}".format(
                        query=line)
                print "original output:\n{orig_output}".format(
                        orig_output=strip_output(orig_output))
                print "bnl output:\n{bnl_output}".format(
                        bnl_output=strip_output(bnl_output))
    end = time.time()
    # Report results
    print ("Ran {tests} tests in {sec} seconds ({passed} passed, {failed} "
            "failed)".format(tests=tests, sec=round(end-start, 1),
                passed=tests-failures, failed=failures))


def main(argv):
    if len(argv) != 2:
        print "Usage: {program_name} <testcase_sql_filename>".format(
                program_name=argv[0])
        exit()
   
    run_tests(argv[1])


if __name__ == '__main__':
    main(sys.argv)
