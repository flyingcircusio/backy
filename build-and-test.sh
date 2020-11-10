#!/bin/sh
set -e

rm -rf bin/ lib/ eggs/ include/ parts/
python3 -m venv .
bin/pip install zc.buildout setuptools==47.3.1
bin/buildout
bin/pytest
