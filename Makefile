
build:
	python3 setup.py


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
	rm -rf test/__pycache__

run_tests: clean
	python3 -m unittest discover -s test

