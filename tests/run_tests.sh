#!/bin/bash
"""
Created on Thu Feb 17 19:04:46 2022

@author: alvaro
"""
rm -f /tests/.coverage
cd /tests && export PYTHONPATH=${PYTHONPATH}:../src:. && py.test test_*.py --cov=../src --cov-report=
