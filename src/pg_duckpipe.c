#include "pg_duckpipe.h"

PG_MODULE_MAGIC;

void _PG_init(void);

int duckpipe_poll_interval = 1000;
int duckpipe_batch_size_per_table = 1000;
int duckpipe_batch_size_per_group = 10000;
bool duckpipe_enabled = true;
bool duckpipe_debug_log = false;
int duckpipe_data_inlining_row_limit = 0;

void
_PG_init(void) {
	DefineCustomIntVariable("duckpipe.poll_interval", "Interval in milliseconds between polls", NULL,
	                        &duckpipe_poll_interval, 1000, 100, 3600000, PGC_SIGHUP, GUC_UNIT_MS, NULL, NULL, NULL);

	DefineCustomIntVariable("duckpipe.batch_size_per_table", "Maximum number of changes per table per batch", NULL,
	                        &duckpipe_batch_size_per_table, 1000, 1, 1000000, PGC_SIGHUP, 0, NULL, NULL, NULL);

	DefineCustomIntVariable("duckpipe.batch_size_per_group", "Maximum number of changes per group per round", NULL,
	                        &duckpipe_batch_size_per_group, 10000, 100, 10000000, PGC_SIGHUP, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable("duckpipe.enabled", "Enable pg_duckpipe background worker", NULL, &duckpipe_enabled, true,
	                         PGC_SIGHUP, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable("duckpipe.debug_log", "Emit critical-path timing logs for pg_duckpipe", NULL,
	                         &duckpipe_debug_log, false, PGC_SIGHUP, 0, NULL, NULL, NULL);

	DefineCustomIntVariable("duckpipe.data_inlining_row_limit",
	                         "DuckLake data inlining row limit (0 = disabled)", NULL,
	                         &duckpipe_data_inlining_row_limit, 0, 0, 1000000, PGC_USERSET, 0, NULL, NULL, NULL);
}
