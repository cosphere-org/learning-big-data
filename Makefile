# Makefile

SHELL := /bin/bash

help:  ## show this help
	@fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sed -e 's/\\$$//' | sed -e 's/##//'

PWD := $(shell pwd)

#
# TESTS
#
test:  ## run selected tests
	py.test --cov=./learning_big_data --cov-config .coveragerc -r w -s -vv $(tests)

test_all:  ## run all available tests
	py.test --cov=./learning_big_data --cov-config .coveragerc -r w -s -vv tests

coverage:  # render html coverage report
	coverage html -d coverage_html && google-chrome coverage_html/index.html


#
# JUPYTER
#
start_jupyter_notebook:  ## start jupyter notebook server with `learning_big_data` directory attached
	docker build -t jupyter/notebook . && \
	docker run \
		-p 8888:8888 \
		-e JUPYTER_LAB_ENABLE=yes \
		-v "${PWD}/learning_big_data":/home/jovyan/work \
		jupyter/notebook

#
# DASK
#
start_dask_slave:  ## run local dask slave which will attach to the remote master
	@echo "to be available soon..."

start_dask_cluster:  ## run local dask cluster
	@echo "to be available soon..."


#
# SPARK
#
start_spark_standalone:  ## run single node local spark cluster
	@echo "to be available soon..."

start_spark_cluster:  ## run multi-node local spark cluster
	@echo "to be available soon..."


#
# MRJOB / HADOOP
#
start_hadoop_cluster:
	@echo "to be available soon..."
