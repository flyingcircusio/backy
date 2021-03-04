#!/bin/sh
set -e

rm -rf bin/ lib/ eggs/ include/ parts/
python3 -m venv .
bin/pip install -r requirements.txt
bin/pip install -e .
bin/pytest
