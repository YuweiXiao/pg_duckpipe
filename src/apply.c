#include "pg_duckpipe.h"

#include "executor/spi.h"
#include "portability/instr_time.h"
#include "utils/builtins.h"

static double
duckpipe_elapsed_ms(const instr_time *start) {
	instr_time end = *start;

	INSTR_TIME_SET_CURRENT(end);
	INSTR_TIME_SUBTRACT(end, *start);
	return INSTR_TIME_GET_MILLISEC(end);
}

/*
 * Quote literal value for SQL using PostgreSQL's quote_literal_cstr.
 * Returns palloc'd string. Handles NULL values properly.
 */
static char *
quote_val(char *val) {
	if (val == NULL)
		return pstrdup("NULL");
	return quote_literal_cstr(val);
}

/*
 * Build a properly quoted table reference (schema.table).
 * Returns palloc'd string.
 */
static char *
quote_qualified_table(const char *schema, const char *table) {
	StringInfoData buf;
	initStringInfo(&buf);
	appendStringInfo(&buf, "%s.%s", quote_identifier(schema), quote_identifier(table));
	return buf.data;
}

/*
 * Parse schema.table from batch->target_table into separate parts.
 * Returns palloc'd strings via out parameters.
 */
static void
parse_target_table(const char *target_table, char **schema, char **table) {
	char *copy = pstrdup(target_table);
	char *dot = strchr(copy, '.');

	if (dot) {
		*dot = '\0';
		*schema = pstrdup(copy);
		*table = pstrdup(dot + 1);
	} else {
		*schema = pstrdup("public");
		*table = pstrdup(copy);
	}
	pfree(copy);
}

void
apply_batch(SyncBatch *batch) {
	StringInfoData insert_buf;
	bool insert_started = false;
	ListCell *lc;
	int ret;
	char *target_schema;
	char *target_table;
	char *quoted_target;
	bool timing_enabled = duckpipe_debug_log;
	instr_time apply_timer;
	double sql_exec_ms = 0.0;
	double sql_build_ms = 0.0;
	int insert_exec_count = 0;
	int delete_exec_count = 0;
	int insert_row_count = 0;
	instr_time build_timer;

	if (!batch || batch->count == 0)
		return;

	if (timing_enabled)
		INSTR_TIME_SET_CURRENT(apply_timer);

	/* apply_batch is always called within an active SPI session from
	 * process_sync_group, so we must not open a nested SPI connection here.
	 * Parse and quote the target table name once */
	parse_target_table(batch->target_table, &target_schema, &target_table);
	quoted_target = quote_qualified_table(target_schema, target_table);

	initStringInfo(&insert_buf);

	foreach (lc, batch->changes) {
		SyncChange *change = (SyncChange *)lfirst(lc);

		if (change->type == SYNC_CHANGE_INSERT) {
			ListCell *vc;
			bool first_col = true;

			if (timing_enabled)
				INSTR_TIME_SET_CURRENT(build_timer);

			if (!insert_started) {
				appendStringInfo(&insert_buf, "INSERT INTO %s VALUES ", quoted_target);
				insert_started = true;
			} else {
				appendStringInfoString(&insert_buf, ", ");
			}

			appendStringInfoChar(&insert_buf, '(');
			foreach (vc, change->col_values) {
				char *val = (char *)lfirst(vc);
				char *quoted = quote_val(val);

				if (!first_col)
					appendStringInfoString(&insert_buf, ", ");
				appendStringInfoString(&insert_buf, quoted);
				first_col = false;

				pfree(quoted);
			}
			appendStringInfoChar(&insert_buf, ')');
			insert_row_count++;

			if (timing_enabled)
				sql_build_ms += duckpipe_elapsed_ms(&build_timer);
		} else if (change->type == SYNC_CHANGE_DELETE) {
			/* Flush inserts first if any */
			if (insert_started) {
				instr_time exec_timer;
				if (timing_enabled)
					INSTR_TIME_SET_CURRENT(exec_timer);
				ret = SPI_execute(insert_buf.data, false, 0);
				if (timing_enabled) {
					sql_exec_ms += duckpipe_elapsed_ms(&exec_timer);
					insert_exec_count++;
				}
				if (ret != SPI_OK_INSERT)
					elog(ERROR, "Failed to execute INSERT batch for %s (ret=%d)", batch->target_table, ret);
				resetStringInfo(&insert_buf);
				insert_started = false;
			}

			/* Execute DELETE - need column names from batch */
			if (batch->attnames == NIL) {
				elog(WARNING, "DELETE skipped: no column names available for %s", batch->target_table);
				continue;
			}

			{
				StringInfoData del_buf;
				bool skip_delete = false;

				if (timing_enabled)
					INSTR_TIME_SET_CURRENT(build_timer);

				initStringInfo(&del_buf);
				appendStringInfo(&del_buf, "DELETE FROM %s WHERE ", quoted_target);

				/* Build WHERE clause using key columns if known, otherwise use all
				 * columns */
				if (batch->nkeyattrs > 0 && batch->keyattrs != NULL) {
					/* Use only key columns.
					 * key_values contains only the replica identity columns in order,
					 * so we use sequential index i for key_values, while attidx is
					 * used to get the column name from the full attnames list.
					 */
					for (int i = 0; i < batch->nkeyattrs; i++) {
						int attidx = batch->keyattrs[i];
						if (i >= list_length(change->key_values) || attidx >= list_length(batch->attnames)) {
							elog(WARNING,
							     "Key bounds error: key_idx=%d (len %d), att_idx=%d (len %d). Skipping DELETE.", i,
							     list_length(change->key_values), attidx, list_length(batch->attnames));
							skip_delete = true;
							break;
						}
					}
					for (int i = 0; i < batch->nkeyattrs && !skip_delete; i++) {
						int attidx = batch->keyattrs[i];

						char *colname = (char *)list_nth(batch->attnames, attidx);
						char *val = (char *)list_nth(change->key_values, i);

						if (i > 0)
							appendStringInfoString(&del_buf, " AND ");

						if (val == NULL) {
							appendStringInfo(&del_buf, "%s IS NULL", quote_identifier(colname));
						} else {
							char *quoted = quote_val(val);
							appendStringInfo(&del_buf, "%s = %s", quote_identifier(colname), quoted);
							pfree(quoted);
						}
					}
				} else {
					/* No key info - use all columns from key_values */
					ListCell *kc, *nc;
					bool first_key = true;

					forboth(kc, change->key_values, nc, batch->attnames) {
						char *val = (char *)lfirst(kc);
						char *colname = (char *)lfirst(nc);

						if (!first_key)
							appendStringInfoString(&del_buf, " AND ");

						/* Handle NULL values in WHERE clause with IS NULL */
						if (val == NULL) {
							appendStringInfo(&del_buf, "%s IS NULL", quote_identifier(colname));
						} else {
							char *quoted = quote_val(val);
							appendStringInfo(&del_buf, "%s = %s", quote_identifier(colname), quoted);
							pfree(quoted);
						}
						first_key = false;
					}
				}

				/* Execute the DELETE unless we detected a bounds error */
				if (timing_enabled)
					sql_build_ms += duckpipe_elapsed_ms(&build_timer);

				if (!skip_delete) {
					instr_time exec_timer;
					if (timing_enabled)
						INSTR_TIME_SET_CURRENT(exec_timer);
					ret = SPI_execute(del_buf.data, false, 0);
					if (timing_enabled) {
						sql_exec_ms += duckpipe_elapsed_ms(&exec_timer);
						delete_exec_count++;
					}
					if (ret != SPI_OK_DELETE)
						elog(WARNING, "Failed to execute DELETE for %s (ret=%d)", batch->target_table, ret);
				}

				pfree(del_buf.data);
			}
		}
	}

	/* Flush remaining inserts */
	if (insert_started) {
		instr_time exec_timer;
		if (timing_enabled)
			INSTR_TIME_SET_CURRENT(exec_timer);
		ret = SPI_execute(insert_buf.data, false, 0);
		if (timing_enabled) {
			sql_exec_ms += duckpipe_elapsed_ms(&exec_timer);
			insert_exec_count++;
		}
		if (ret != SPI_OK_INSERT)
			elog(ERROR, "Failed to execute INSERT batch for %s (ret=%d)", batch->target_table, ret);
	}

	pfree(insert_buf.data);
	pfree(quoted_target);
	pfree(target_schema);
	pfree(target_table);

	if (timing_enabled) {
		elog(LOG,
		     "DuckPipe timing: action=apply_batch table=%s changes=%d insert_rows=%d insert_stmts=%d "
		     "delete_stmts=%d sql_build_ms=%.3f sql_exec_ms=%.3f elapsed_ms=%.3f",
		     batch->target_table, batch->count, insert_row_count, insert_exec_count, delete_exec_count, sql_build_ms,
		     sql_exec_ms, duckpipe_elapsed_ms(&apply_timer));
	}
}
