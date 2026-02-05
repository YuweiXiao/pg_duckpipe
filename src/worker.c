#include "pg_ducklake_sync.h"
#include "access/xlog.h"
#include "executor/spi.h"
#include "utils/memutils.h"
#include "catalog/pg_type.h"
#include "utils/pg_lsn.h"
#include "libpq/pqformat.h"
#include "utils/wait_classes.h"

static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

/* Memory context for sync operations */
static MemoryContext SyncMemoryContext = NULL;

static void
ducklake_sync_sighup(SIGNAL_ARGS)
{
    int save_errno = errno;
    got_sighup = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

static void
ducklake_sync_sigterm(SIGNAL_ARGS)
{
    int save_errno = errno;
    got_sigterm = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

/* Get all enabled sync groups from metadata */
List *
get_enabled_sync_groups(void)
{
    List *groups = NIL;
    int ret;

    ret = SPI_execute(
        "SELECT id, name, publication, slot_name FROM ducklake_sync.sync_groups WHERE enabled = true",
        true, 0);

    if (ret == SPI_OK_SELECT)
    {
        for (uint64 i = 0; i < SPI_processed; i++)
        {
            SyncGroup *group = palloc0(sizeof(SyncGroup));
            bool isnull;

            group->id = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull));
            group->name = pstrdup(TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2, &isnull)));
            group->publication = pstrdup(TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 3, &isnull)));
            group->slot_name = pstrdup(TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 4, &isnull)));
            group->pending_lsn = 0;

            groups = lappend(groups, group);
        }
    }

    return groups;
}

/* Look up table mapping by source schema and table name */
TableMapping *
get_table_mapping(SyncGroup *group, char *schemaname, char *relname)
{
    TableMapping *mapping = NULL;
    Datum values[3];
    Oid argtypes[3] = {INT4OID, TEXTOID, TEXTOID};
    bool isnull;
    int ret;

    values[0] = Int32GetDatum(group->id);
    values[1] = CStringGetTextDatum(schemaname);
    values[2] = CStringGetTextDatum(relname);

    ret = SPI_execute_with_args(
        "SELECT id, group_id, source_schema, source_table, target_schema, target_table, "
        "       state, snapshot_lsn, enabled "
        "FROM ducklake_sync.table_mappings "
        "WHERE group_id = $1 AND source_schema = $2 AND source_table = $3",
        3, argtypes, values, NULL, true, 1);

    if (ret == SPI_OK_SELECT && SPI_processed > 0)
    {
        HeapTuple tuple = SPI_tuptable->vals[0];
        TupleDesc tupdesc = SPI_tuptable->tupdesc;

        mapping = palloc0(sizeof(TableMapping));
        mapping->id = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 1, &isnull));
        mapping->group_id = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 2, &isnull));
        mapping->source_schema = pstrdup(TextDatumGetCString(SPI_getbinval(tuple, tupdesc, 3, &isnull)));
        mapping->source_table = pstrdup(TextDatumGetCString(SPI_getbinval(tuple, tupdesc, 4, &isnull)));
        mapping->target_schema = pstrdup(TextDatumGetCString(SPI_getbinval(tuple, tupdesc, 5, &isnull)));
        mapping->target_table = pstrdup(TextDatumGetCString(SPI_getbinval(tuple, tupdesc, 6, &isnull)));
        mapping->state = pstrdup(TextDatumGetCString(SPI_getbinval(tuple, tupdesc, 7, &isnull)));

        Datum lsn_datum = SPI_getbinval(tuple, tupdesc, 8, &isnull);
        mapping->snapshot_lsn = isnull ? InvalidXLogRecPtr : DatumGetLSN(lsn_datum);

        mapping->enabled = DatumGetBool(SPI_getbinval(tuple, tupdesc, 9, &isnull));
    }

    return mapping;
}

/* Create hash table for batches */
static HTAB *
create_batch_hash(void)
{
    HASHCTL hashctl;

    memset(&hashctl, 0, sizeof(hashctl));
    hashctl.keysize = NAMEDATALEN * 2;  /* schema.table */
    hashctl.entrysize = sizeof(SyncBatch);
    hashctl.hcxt = SyncMemoryContext;

    return hash_create("SyncBatches", 32, &hashctl, HASH_ELEM | HASH_STRINGS | HASH_CONTEXT);
}

/* Create hash table for relation cache */
static HTAB *
create_rel_cache_hash(void)
{
    HASHCTL hashctl;

    memset(&hashctl, 0, sizeof(hashctl));
    hashctl.keysize = sizeof(LogicalRepRelId);
    hashctl.entrysize = sizeof(RelationCacheEntry);
    hashctl.hcxt = SyncMemoryContext;

    return hash_create("RelationCache", 32, &hashctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

/* Process one sync group - read changes from slot and apply */
int
process_sync_group(SyncGroup *group)
{
    int total_processed = 0;
    int ret;
    HTAB *batches;
    HTAB *rel_cache;
    StringInfoData query;

    /* Create hash tables for this processing round */
    batches = create_batch_hash();
    rel_cache = create_rel_cache_hash();

    /* Query the replication slot for binary changes */
    initStringInfo(&query);
    appendStringInfo(&query,
        "SELECT lsn, data FROM pg_logical_slot_get_binary_changes("
        "'%s', NULL, %d, 'proto_version', '1', 'publication_names', '%s')",
        group->slot_name,
        ducklake_sync_batch_size_per_group,
        group->publication);

    ret = SPI_execute(query.data, true, 0);
    pfree(query.data);

    if (ret != SPI_OK_SELECT)
    {
        elog(WARNING, "Failed to get changes from slot %s", group->slot_name);
        hash_destroy(batches);
        hash_destroy(rel_cache);
        return 0;
    }

    /* Process each change message */
    for (uint64 i = 0; i < SPI_processed; i++)
    {
        bool isnull;
        Datum lsn_datum = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull);
        Datum data_datum = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2, &isnull);

        if (isnull)
            continue;

        XLogRecPtr lsn = DatumGetLSN(lsn_datum);
        bytea *data = DatumGetByteaP(data_datum);
        int len = VARSIZE(data) - VARHDRSZ;
        char *raw = VARDATA(data);

        /* Create StringInfo for parsing */
        StringInfoData buf;
        initStringInfo(&buf);
        appendBinaryStringInfo(&buf, raw, len);

        /* Decode and process the message */
        decode_message(&buf, lsn, group, batches, rel_cache);

        pfree(buf.data);
        total_processed++;
    }

    /* Flush any remaining batches */
    flush_all_batches(batches);

    /* Clean up hash tables */
    hash_destroy(batches);
    hash_destroy(rel_cache);

    return total_processed;
}

void
ducklake_sync_worker_main(Datum main_arg)
{
    /* Set up signal handlers */
    pqsignal(SIGHUP, ducklake_sync_sighup);
    pqsignal(SIGTERM, ducklake_sync_sigterm);
    BackgroundWorkerUnblockSignals();

    /* Connect to database */
    BackgroundWorkerInitializeConnection("postgres", NULL, 0);

    /* Create memory context for sync operations */
    SyncMemoryContext = AllocSetContextCreate(TopMemoryContext,
                                              "pg_ducklake_sync",
                                              ALLOCSET_DEFAULT_SIZES);

    elog(LOG, "pg_ducklake_sync worker started");

    while (!got_sigterm)
    {
        int rc;
        List *groups;
        ListCell *lc;
        bool any_work = false;

        /* Reload config if signaled */
        if (got_sighup)
        {
            got_sighup = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        /* Skip if disabled */
        if (!ducklake_sync_enabled)
        {
            rc = WaitLatch(MyLatch,
                           WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                           ducklake_sync_poll_interval,
                           PG_WAIT_EXTENSION);
            ResetLatch(MyLatch);
            continue;
        }

        /* Process sync groups */
        StartTransactionCommand();
        SPI_connect();

        groups = get_enabled_sync_groups();

        foreach(lc, groups)
        {
            SyncGroup *group = (SyncGroup *) lfirst(lc);
            int processed = process_sync_group(group);
            if (processed > 0)
                any_work = true;
        }

        SPI_finish();
        CommitTransactionCommand();

        /* Reset memory context after each round */
        MemoryContextReset(SyncMemoryContext);

        /* Wait before next poll (shorter if there was work) */
        rc = WaitLatch(MyLatch,
                       WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                       any_work ? 10 : ducklake_sync_poll_interval,
                       PG_WAIT_EXTENSION);
        ResetLatch(MyLatch);
    }

    elog(LOG, "pg_ducklake_sync worker shutting down");
    proc_exit(0);
}
