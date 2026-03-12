PYTHON := $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)
BACKEND_PORT ?= 8000
FRONTEND_PORT ?= 5173
BACKEND_HOST ?= 127.0.0.1
FRONTEND_HOST ?= 127.0.0.1

.PHONY: backend-install frontend-install install run-backend run-frontend run-all

backend-install:
	cd backend && $(PYTHON) -m pip install -r requirements.txt

frontend-install:
	npm --prefix frontend install

install: backend-install frontend-install

run-backend:
	$(PYTHON) -m uvicorn app.main:app --app-dir backend --host $(BACKEND_HOST) --port $(BACKEND_PORT) --reload

run-frontend:
	npm --prefix frontend run dev -- --host $(FRONTEND_HOST) --port $(FRONTEND_PORT)

run-all:
	@$(PYTHON) -m uvicorn app.main:app --app-dir backend --host $(BACKEND_HOST) --port $(BACKEND_PORT) --reload & \
	BACK_PID=$$!; \
	trap 'kill $$BACK_PID 2>/dev/null' EXIT INT TERM; \
	npm --prefix frontend run dev -- --host $(FRONTEND_HOST) --port $(FRONTEND_PORT)