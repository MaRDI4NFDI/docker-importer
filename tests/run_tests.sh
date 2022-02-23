#!/bin/bash
rm -f /tests/.coverage
cd /tests &&\
export PYTHONPATH=${PYTHONPATH}:../src:. &&\
pytest test_*.py --cov=../src --cov-report term
