PYTHON = python3
PYPI_USER = aquavitale

upload: build
	twine upload -u $(PYPI_USER) dist/*

build: clean
	bumpversion patch
	$(PYTHON) setup.py sdist bdist_wheel

clean:
	rm -f dist/*
