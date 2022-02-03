#!/usr/bin/env sh

coverage erase && coverage run -m unittest discover && coverage report
