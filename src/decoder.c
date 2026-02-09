#include "pg_duckpipe.h"

#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/memutils.h"

/*
 * Helper to convert LogicalRepTupleData to a List of string values.
 * We deep copy the strings because the buffer is transient.
 * Allocates in SyncMemoryContext so data survives SPI commits.
 *
 * LogicalRepTupleData structure (this PostgreSQL version):
 *   StringInfoData *colvalues; -- array of StringInfoData, one per column
 *   char *colstatus;           -- status per column ('n'=null, 'u'=unchanged,
 * 't'=text, 'b'=binary) int ncols;                 -- number of columns
 */
static List *
tuple_to_list(LogicalRepTupleData *tuple, LogicalRepRelation *rel) {
	List *values = NIL;
	int i;
	MemoryContext oldcxt = MemoryContextSwitchTo(SyncMemoryContext);

	for (i = 0; i < tuple->ncols; i++) {
		char status = tuple->colstatus[i];
		char *val_str = NULL;

		if (status == 'n') /* NULL */
		{
			val_str = NULL;
		} else if (status == 'u') /* Unchanged (TOAST) */
		{
			/* Treat as NULL for now - for INSERT this shouldn't happen */
			val_str = NULL;
		} else /* 't' (text) or 'b' (binary) */
		{
			/* colvalues is an array of StringInfoData */
			int len = tuple->colvalues[i].len;
			char *data = tuple->colvalues[i].data;

			val_str = palloc(len + 1);
			memcpy(val_str, data, len);
			val_str[len] = '\0';
		}

		values = lappend(values, val_str);
	}

	MemoryContextSwitchTo(oldcxt);
	return values;
}

/*
 * Extract only the primary key column values from a tuple.
 * Used for UPDATE when !has_old: newtup contains all columns but
 * key_values should only contain the PK columns in order.
 * Allocates in SyncMemoryContext so data survives SPI commits.
 */
static List *
extract_key_values(LogicalRepTupleData *tuple, LogicalRepRelation *rel) {
	List *values = NIL;
	int x = -1;
	MemoryContext oldcxt = MemoryContextSwitchTo(SyncMemoryContext);

	if (rel->attkeys == NULL) {
		/* No key info - fall back to full tuple */
		MemoryContextSwitchTo(oldcxt);
		return tuple_to_list(tuple, rel);
	}

	while ((x = bms_next_member(rel->attkeys, x)) >= 0) {
		if (x < tuple->ncols) {
			char status = tuple->colstatus[x];
			char *val_str = NULL;

			if (status == 'n' || status == 'u') {
				val_str = NULL;
			} else {
				int len = tuple->colvalues[x].len;
				char *data = tuple->colvalues[x].data;

				val_str = palloc(len + 1);
				memcpy(val_str, data, len);
				val_str[len] = '\0';
			}

			values = lappend(values, val_str);
		}
	}

	MemoryContextSwitchTo(oldcxt);
	return values;
}

/*
 * Free a deep-copied LogicalRepRelation and its contents.
 */
static void
free_logical_rep_relation(LogicalRepRelation *rel) {
	if (rel == NULL)
		return;

	if (rel->nspname)
		pfree(rel->nspname);
	if (rel->relname)
		pfree(rel->relname);
	if (rel->attnames) {
		for (int i = 0; i < rel->natts; i++) {
			if (rel->attnames[i])
				pfree(rel->attnames[i]);
		}
		pfree(rel->attnames);
	}
	if (rel->atttyps)
		pfree(rel->atttyps);
	if (rel->attkeys)
		bms_free(rel->attkeys);
	pfree(rel);
}

bool
decode_message(StringInfo buf, XLogRecPtr lsn, SyncGroup *group, HTAB *batches, HTAB *rel_cache) {
	char msgtype = pq_getmsgbyte(buf);

	switch (msgtype) {
	case LOGICAL_REP_MSG_RELATION: {
		LogicalRepRelation *rel = logicalrep_read_rel(buf);
		RelationCacheEntry *entry;
		bool found;

		entry = (RelationCacheEntry *)hash_search(rel_cache, &rel->remoteid, HASH_ENTER, &found);
		if (found && entry->rel != NULL) {
			/* Free old relation data to prevent memory leak */
			free_logical_rep_relation(entry->rel);
		}
		entry->rel = rel;      /* Store the new relation definition */
		entry->mapping = NULL; /* Reset cached mapping on schema change */
		break;
	}
	case LOGICAL_REP_MSG_INSERT: {
		LogicalRepTupleData newtup;
		LogicalRepRelId relid;
		TableMapping *mapping;
		RelationCacheEntry *entry;
		SyncChange *change;

		relid = logicalrep_read_insert(buf, &newtup);

		/* Find Relation info */
		entry = (RelationCacheEntry *)hash_search(rel_cache, &relid, HASH_FIND, NULL);
		if (!entry)
			return false; /* Should not happen if protocol flow is correct */

		/* Find Table Mapping (cached per relation per poll round) */
		if (entry->mapping == NULL)
			entry->mapping = get_table_mapping(group, entry->rel->nspname, entry->rel->relname);
		mapping = entry->mapping;
		if (!mapping || !mapping->enabled)
			return false;

		/* During CATCHUP, skip changes already included in snapshot */
		if (mapping->state == SYNC_STATE_CATCHUP && mapping->snapshot_lsn != InvalidXLogRecPtr &&
		    lsn <= mapping->snapshot_lsn)
			return false;

		/* Construct Change in SyncMemoryContext */
		{
			MemoryContext oldcxt = MemoryContextSwitchTo(SyncMemoryContext);
			change = palloc(sizeof(SyncChange));
			change->type = SYNC_CHANGE_INSERT;
			change->lsn = lsn;
			change->key_values = NIL;
			MemoryContextSwitchTo(oldcxt);
		}
		change->col_values = tuple_to_list(&newtup, entry->rel);

		batch_add_change(batches, mapping, change, entry->rel);
		break;
	}
	case LOGICAL_REP_MSG_UPDATE: {
		LogicalRepTupleData oldtup, newtup;
		bool has_old;
		LogicalRepRelId relid;
		TableMapping *mapping;
		RelationCacheEntry *entry;
		SyncChange *change;

		relid = logicalrep_read_update(buf, &has_old, &oldtup, &newtup);

		entry = (RelationCacheEntry *)hash_search(rel_cache, &relid, HASH_FIND, NULL);
		if (!entry)
			return false;
		if (entry->mapping == NULL)
			entry->mapping = get_table_mapping(group, entry->rel->nspname, entry->rel->relname);
		mapping = entry->mapping;
		if (!mapping || !mapping->enabled)
			return false;

		/* During CATCHUP, skip changes already included in snapshot */
		if (mapping->state == SYNC_STATE_CATCHUP && mapping->snapshot_lsn != InvalidXLogRecPtr &&
		    lsn <= mapping->snapshot_lsn)
			return false;

		/* Treat UPDATE as DELETE + INSERT for simplicity in V1 (easy for column
		 * store) */
		/* Always generate DELETE first.
		   If has_old is true, use oldtup for key.
		   If has_old is false, extract only PK columns from newtup.
		*/
		{
			MemoryContext oldcxt = MemoryContextSwitchTo(SyncMemoryContext);
			SyncChange *del_change = palloc(sizeof(SyncChange));
			del_change->type = SYNC_CHANGE_DELETE;
			del_change->lsn = lsn;
			del_change->col_values = NIL; /* Explicitly initialize */
			MemoryContextSwitchTo(oldcxt);

			if (has_old)
				del_change->key_values = tuple_to_list(&oldtup, entry->rel);
			else
				del_change->key_values = extract_key_values(&newtup, entry->rel);

			batch_add_change(batches, mapping, del_change, entry->rel);
		}

		{
			MemoryContext oldcxt = MemoryContextSwitchTo(SyncMemoryContext);
			change = palloc(sizeof(SyncChange));
			change->type = SYNC_CHANGE_INSERT;
			change->lsn = lsn;
			change->key_values = NIL; /* Explicitly initialize */
			MemoryContextSwitchTo(oldcxt);
		}
		change->col_values = tuple_to_list(&newtup, entry->rel);
		batch_add_change(batches, mapping, change, entry->rel);
		break;
	}
	case LOGICAL_REP_MSG_DELETE: {
		LogicalRepTupleData oldtup;
		LogicalRepRelId relid;
		TableMapping *mapping;
		RelationCacheEntry *entry;
		SyncChange *change;

		relid = logicalrep_read_delete(buf, &oldtup);

		entry = (RelationCacheEntry *)hash_search(rel_cache, &relid, HASH_FIND, NULL);
		if (!entry)
			return false;
		if (entry->mapping == NULL)
			entry->mapping = get_table_mapping(group, entry->rel->nspname, entry->rel->relname);
		mapping = entry->mapping;
		if (!mapping || !mapping->enabled)
			return false;

		/* During CATCHUP, skip changes already included in snapshot */
		if (mapping->state == SYNC_STATE_CATCHUP && mapping->snapshot_lsn != InvalidXLogRecPtr &&
		    lsn <= mapping->snapshot_lsn)
			return false;

		{
			MemoryContext oldcxt = MemoryContextSwitchTo(SyncMemoryContext);
			change = palloc(sizeof(SyncChange));
			change->type = SYNC_CHANGE_DELETE;
			change->lsn = lsn;
			change->col_values = NIL; /* Explicitly initialize */
			MemoryContextSwitchTo(oldcxt);
		}
		change->key_values = tuple_to_list(&oldtup, entry->rel);
		batch_add_change(batches, mapping, change, entry->rel);
		break;
	}
	case LOGICAL_REP_MSG_BEGIN: {
		LogicalRepBeginData begin_data;
		logicalrep_read_begin(buf, &begin_data);
		break;
	}
	case LOGICAL_REP_MSG_COMMIT: {
		LogicalRepCommitData commit_data;
		logicalrep_read_commit(buf, &commit_data);
		group->pending_lsn = commit_data.end_lsn;
		/* Do not flush on every source COMMIT.
		 * We batch across multiple source transactions and flush at:
		 *  1) batch_size_per_table threshold
		 *  2) TRUNCATE boundaries
		 *  3) end-of-poll round in process_sync_group()
		 * This avoids generating one tiny DuckLake file per source commit. */
		return true; /* Signal caller that a commit boundary was reached */
	}
	case LOGICAL_REP_MSG_TRUNCATE: {
		/*
		 * Handle source TRUNCATE by clearing all target tables involved.
		 * Use DELETE for compatibility with ducklake targets.
		 * logicalrep_read_truncate returns a List of LogicalRepRelId values.
		 */
		bool cascade;
		bool restart_seqs;
		List *relid_list;
		ListCell *lc;

		relid_list = logicalrep_read_truncate(buf, &cascade, &restart_seqs);
		(void)cascade;
		(void)restart_seqs;

		/* Flush pending batches before TRUNCATE */
		flush_all_batches(batches);

		/* Execute DELETEs within the caller's SPI session */
		foreach (lc, relid_list) {
			LogicalRepRelId relid = lfirst_oid(lc);
			RelationCacheEntry *entry = (RelationCacheEntry *)hash_search(rel_cache, &relid, HASH_FIND, NULL);
			if (entry) {
				TableMapping *mapping = get_table_mapping(group, entry->rel->nspname, entry->rel->relname);
				if (mapping && mapping->enabled) {
					StringInfoData clear_buf;
					initStringInfo(&clear_buf);
					appendStringInfo(&clear_buf, "DELETE FROM %s.%s", quote_identifier(mapping->target_schema),
					                 quote_identifier(mapping->target_table));

					if (SPI_execute(clear_buf.data, false, 0) != SPI_OK_DELETE)
						elog(WARNING, "Failed to clear target table %s.%s", mapping->target_schema,
						     mapping->target_table);
					else
						elog(LOG, "DuckPipe: Cleared %s.%s due to source TRUNCATE", mapping->target_schema,
						     mapping->target_table);

					pfree(clear_buf.data);
				}
			}
		}

		list_free(relid_list);
		break;
	}
	default:
		elog(DEBUG1, "pg_duckpipe: unknown message type %c", msgtype);
		break;
	}

	return false;
}
