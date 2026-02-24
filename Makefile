PG_CONFIG = /Users/xiaoyuwei/Desktop/workspace_ducklake/postgres/work/app/bin/pg_config

.PHONY: build install check-regression clean-regression installcheck format clean

build:
	cd duckpipe-pg && cargo pgrx install --pg-config=$(PG_CONFIG)

install: build

check-regression:
	$(MAKE) -C test/regression check-regression

clean-regression:
	$(MAKE) -C test/regression clean-regression

installcheck: install
	$(MAKE) check-regression

format:
	cd duckpipe-pg && cargo fmt

clean:
	cd duckpipe-pg && cargo clean
