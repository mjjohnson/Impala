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


def create_test_table(table_name):
    """Create a table with additional test data."""
    sql = """DROP TABLE IF EXISTS {name};
                CREATE EXTERNAL TABLE {name}
                (
                       id INT,
                       col_1 BOOLEAN,
                       col_2 DOUBLE,
                       col_3 TIMESTAMP,
                       col_4 STRING
                )
                ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
                LOCATION '/home/cloudera/csv/';""".format(name=table_name)
    run_query(sql, False)


def run_tests(results_filename, test_filename):
    """Run the Impala tests contained in filename.

    Args:
        filename: A string containing the name of a file that has SQL
            statements to be run on Impala and compared with and without the
            custom block nested join enabled.
    """
    for table_name in ('basictest', 'testlong', 'testmissing', 'testextra',
            'testescape'):
        create_test_table(table_name)

    tests = 0
    failures = 0
    # Time how long the tests take to run
    start = time.time()
    result_blocks = []
    current_results = []
    block_delimiter = "***** ***** ***** *****"
    
    with open(results_filename, 'rU') as f:
        for line in f:
            line = line.strip()
            if line == block_delimiter:
                result_blocks.append('\n'.join(current_results))
                current_results = []
            else:
                current_results.append(line)

    with open(test_filename, 'rU') as f:
        for line in f:
            line = line.strip()
            # Ignore blank lines and treat -- as commented lines.
            if not line or line.startswith('--'):
                continue

            tests += 1
            output = strip_output(run_query(line, True))

            # This output should match the known-correct output unless we've
            # broken something.
            if output != result_blocks[tests-1]:
                failures += 1
                print "got different output for query: {query}".format(
                        query=line)
                print "original output:\n{orig_output}".format(
                        orig_output=result_blocks[tests-1])
                print "current output:\n{output}".format(
                        output=output)
    end = time.time()
    # Report results
    print ("Ran {tests} tests in {sec} seconds ({passed} passed, {failed} "
            "failed)".format(tests=tests, sec=round(end-start, 1),
                passed=tests-failures, failed=failures))


def main(argv):
    if len(argv) != 3:
        print ("Usage: {program_name} <results_filename> "
               "<testcase_sql_filename>").format(program_name=argv[0])
        exit()
   
    run_tests(argv[1], argv[2])


if __name__ == '__main__':
    main(sys.argv)

