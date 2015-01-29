#!/usr/bin/env python

import sys
import platform
from subprocess import Popen, PIPE, STDOUT

def main():
    if Popen(['redis-cli', 'info'], stdout=PIPE).wait() != 0:
        raise RuntimeError("Redis server is not running.")

    pipe = "rg" if Popen(['which', 'rg'], stdout=PIPE).wait() == 0 else "cat"
    module = "discover" if platform.python_version() < '2.7' else 'unittest discover'

    #run tests and gather output
    p = Popen("/usr/bin/env python -m %s -v -s tests %s" % (module, " ".join(sys.argv[1:])), shell=True, stdout=PIPE, stderr=STDOUT)
    exit_code = p.wait()
    (out, _) = p.communicate()

    #filter through egrep & rg/cat
    p2 = Popen("egrep -v '^test_' | %s" % pipe, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT)
    (out, _) = p2.communicate(input=out)

    #print output and return exit code
    print(out.decode())
    return exit_code

if __name__ == "__main__":
    sys.exit(main())
