#!/usr/bin/env python

import os
from subprocess import Popen, PIPE
import sys
from StringIO import StringIO


def get_output(query, enable_custom_op):
    p = Popen([os.environ["IMPALA_HOME"] + "/bin/impala-shell.sh"], stdin=PIPE,
            stdout=PIPE, stderr=PIPE)
    input = "set enable_custom_op={custom}; {query}".format(
            custom='true' if enable_custom_op else 'false', query=query)
    output = p.communicate(input=input)[0]
    return output


def strip_output(output):
    output_lines = output.splitlines()[6:-1]
    return '\n'.join(output_lines)


def run_tests(filename):
    with open(filename, 'rU') as f:
        failures = 0
        for line in f:
            orig_output = get_output(line, False)
            bnl_output = get_output(line, True)

            print strip_output(bnl_output)
            if orig_output != bnl_output:
                failures += 1
                print "got different output for query: {query}".format(
                        query=line)
                print "original output:\n{orig_output}".format(
                        orig_output=strip_output(orig_output))
                print "bnl output:\n{bnl_output}".format(
                        bnl_output=strip_output(bnl_output))
    if failures:
        print "{failures} tests failed".format(failures=failures)


def main(argv):
    if len(argv) != 2:
        print "Usage: {program_name} <testcase_sql_filename>".format(
                program_name=argv[0])
        exit()
   
    run_tests(argv[1])


if __name__ == '__main__':
    main(sys.argv)
