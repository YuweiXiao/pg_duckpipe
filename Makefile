MODULE_big = pg_ducklake_sync
OBJS = src/pg_ducklake_sync.o src/worker.o src/decoder.o src/batch.o src/apply.o src/api.o

EXTENSION = pg_ducklake_sync
DATA = sql/pg_ducklake_sync--1.0.sql
PGFILEDESC = "pg_ducklake_sync - PostgreSQL HTAP Sync Extension"

REGRESS = basic
REGRESS_OPTS = --temp-config=./test/ducklake_sync.conf --inputdir=test

PG_CONFIG = /Users/xiaoyuwei/Desktop/workspace_ducklake/postgres/work/app/bin/pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
