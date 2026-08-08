.PHONY: backup restore verify backup-list test

VENV := . .venv/bin/activate &&

backup:
	$(VENV) python -m idx.jobs.backup backup

backup-list:
	$(VENV) python -m idx.jobs.backup list

verify:
	$(VENV) python -m idx.jobs.backup verify

# Usage: make restore FILE=data/backups/idx_backup_....dump TARGET=idx_restored
restore:
	$(VENV) python -m idx.jobs.backup restore $(FILE) --target-db $(TARGET)

test:
	$(VENV) pytest tests/ -q
