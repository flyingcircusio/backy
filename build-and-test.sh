#!/bin/sh
set -e

cd "$(dirname "$0")"

poetry install
poetry run pytest
