PACKAGE_NAME := noidd
MODULE_PATH := $(shell pwd)/$(PACKAGE_NAME)
CURRENT_VERSION := $(shell cat $(MODULE_PATH)/__init__.py | grep -Eo "[0-9]\.[0-9]\.[0-9]")

bump-version:
	current_version=$(CURRENT_VERSION)
	new_version=$(shell semver bump minor $(current_version))
	echo $(new_version)


build:
	python3 setup.py bdist_wheel

reqs:
	pipreqs . --force

clean:
	rm -rf *.egg-info
	rm -rf build
	rm -rf dist
	rm -rf .pytest_cache
	# Remove all pycache
	find . | grep -E "(__pycache__|\.pyc|\.pyo)" | xargs rm -rf
	rm -rf test/test_files

run_tests: clean
	python3 -m unittest discover -s test
test: run_tests clean
