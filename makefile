APPLICATION_MODULE_NAME = didomi

VENV_ACTIVATE_FILE = ./activate_venv


venv:  ## Create virtualenv
	python -m venv venv
	. $(VENV_ACTIVATE_FILE) && pip install -U -e . -r requirements-dev.txt

update: venv  ## Update dependencies
	. $(VENV_ACTIVATE_FILE) && pip install -U -e . -r requirements-dev.txt

