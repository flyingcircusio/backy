#!/bin/sh
set -x
rm -rf bin/ lib/ eggs/ include/ parts/
python3 -m venv .
bin/pip install zc.buildout
bin/buildout
bin/py.test -m1 --junitxml=parts/tests.xml
