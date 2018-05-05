#!/bin/sh
flake8 --max-line-length=99 --ignore=W291,W293,E121,E123,E124,E126,E127,E128,E131,E231,E302,E502 *.py
mypy --ignore-missing-imports *.py
