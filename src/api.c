#include "pg_ducklake_sync.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "executor/spi.h"
#include "catalog/pg_type.h"

/* Helper to return empty set for SRFs */
static Datum
empty_set(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;

    if (SRF_IS_FIRSTCALL())
    {
        MemoryContext oldcontext;
        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    SRF_RETURN_DONE(funcctx);
}

PG_FUNCTION_INFO_V1(ducklake_sync_create_group);
Datum ducklake_sync_create_group(PG_FUNCTION_ARGS)
{
    char *name = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char *pub = PG_ARGISNULL(1) ? psprintf("ducklake_sync_pub_%s", name) : text_to_cstring(PG_GETARG_TEXT_PP(1));
    char *slot = PG_ARGISNULL(2) ? psprintf("ducklake_sync_slot_%s", name) : text_to_cstring(PG_GETARG_TEXT_PP(2));
    StringInfoData buf;
    int ret;
    Datum values[3];
    Oid argtypes[3] = {TEXTOID, TEXTOID, TEXTOID};
    char nulls[3] = {' ', ' ', ' '};

    if ((ret = SPI_connect()) != SPI_OK_CONNECT)
        elog(ERROR, "SPI_connect failed: %d", ret);

    /* 1. Create slot (Must be first to avoid transaction write conflict) */
    initStringInfo(&buf);
    appendStringInfo(&buf, "SELECT pg_create_logical_replication_slot('%s', 'pgoutput')", slot);
    ret = SPI_execute(buf.data, false, 0);
    if (ret < 0) elog(ERROR, "Failed to create slot %s", slot);

    /* 2. Create publication */
    resetStringInfo(&buf);
    appendStringInfo(&buf, "CREATE PUBLICATION %s", pub);
    ret = SPI_execute(buf.data, false, 0);
    if (ret < 0) elog(ERROR, "Failed to create publication %s", pub);

    /* 3. Insert into sync_groups */
    values[0] = CStringGetTextDatum(name);
    values[1] = CStringGetTextDatum(pub);
    values[2] = CStringGetTextDatum(slot);

    ret = SPI_execute_with_args(
        "INSERT INTO ducklake_sync.sync_groups (name, publication, slot_name) VALUES ($1, $2, $3)",
        3, argtypes, values, nulls, false, 0);
        
    if (ret != SPI_OK_INSERT)
        elog(ERROR, "Failed to insert into sync_groups");
        
    SPI_finish();

    PG_RETURN_TEXT_P(cstring_to_text(name));
}

PG_FUNCTION_INFO_V1(ducklake_sync_drop_group);
Datum ducklake_sync_drop_group(PG_FUNCTION_ARGS)
{
    char *name = text_to_cstring(PG_GETARG_TEXT_PP(0));
    bool drop_slot = PG_GETARG_BOOL(1);
    char *pub;
    char *slot;
    int ret;
    bool isnull;

    if ((ret = SPI_connect()) != SPI_OK_CONNECT)
        elog(ERROR, "SPI_connect failed");

    {
        Datum values[1] = {CStringGetTextDatum(name)};
        Oid argtypes[1] = {TEXTOID};
        if (SPI_execute_with_args("SELECT publication, slot_name FROM ducklake_sync.sync_groups WHERE name = $1", 
                                  1, argtypes, values, NULL, true, 0) != SPI_OK_SELECT)
            elog(ERROR, "Failed to query sync_groups");
            
        if (SPI_processed == 0)
            elog(ERROR, "Sync group not found: %s", name);
            
        pub = TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
        slot = TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull));
    }
    
    if (drop_slot) {
        StringInfoData buf;
        initStringInfo(&buf);
        appendStringInfo(&buf, "SELECT pg_drop_replication_slot('%s')", slot);
        ret = SPI_execute(buf.data, false, 0);
        if (ret < 0) elog(WARNING, "Failed to drop slot %s", slot);
    }
    
    {
        StringInfoData buf;
        initStringInfo(&buf);
        appendStringInfo(&buf, "DROP PUBLICATION IF EXISTS %s", pub);
        ret = SPI_execute(buf.data, false, 0);
        if (ret < 0) elog(WARNING, "Failed to drop publication %s", pub);
    }

    {
        Datum values[1] = {CStringGetTextDatum(name)};
        Oid argtypes[1] = {TEXTOID};
        SPI_execute_with_args("DELETE FROM ducklake_sync.sync_groups WHERE name = $1", 1, argtypes, values, NULL, false, 0);
    }
    
    SPI_finish();

    PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(ducklake_sync_enable_group);
Datum ducklake_sync_enable_group(PG_FUNCTION_ARGS) { PG_RETURN_VOID(); }

PG_FUNCTION_INFO_V1(ducklake_sync_disable_group);
Datum ducklake_sync_disable_group(PG_FUNCTION_ARGS) { PG_RETURN_VOID(); }

PG_FUNCTION_INFO_V1(ducklake_sync_add_table);
Datum ducklake_sync_add_table(PG_FUNCTION_ARGS)
{
    char *source_table_arg = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char *target_table_arg = PG_ARGISNULL(1) ? NULL : text_to_cstring(PG_GETARG_TEXT_PP(1));
    char *group = PG_ARGISNULL(2) ? "default" : text_to_cstring(PG_GETARG_TEXT_PP(2));

    char *source_table_copy = pstrdup(source_table_arg);
    char *schema = "public";
    char *table = source_table_copy;
    char *dot = strchr(source_table_copy, '.');
    if (dot) {
        *dot = '\0';
        schema = source_table_copy;
        table = dot + 1;
    }

    char *t_schema = "ducklake";
    char *t_table;
    if (!target_table_arg) {
        t_table = pstrdup(table);
    } else {
        char *target_copy = pstrdup(target_table_arg);
        char *t_dot = strchr(target_copy, '.');
        if (t_dot) {
            *t_dot = '\0';
            t_schema = target_copy;
            t_table = t_dot + 1;
        } else {
            t_table = target_copy;
        }
    }

    if (SPI_connect() != SPI_OK_CONNECT) elog(ERROR, "SPI_connect failed");

    /* Use parameterized query to prevent SQL injection */
    {
        Datum values[5];
        Oid argtypes[5] = {TEXTOID, TEXTOID, TEXTOID, TEXTOID, TEXTOID};

        values[0] = CStringGetTextDatum(schema);
        values[1] = CStringGetTextDatum(table);
        values[2] = CStringGetTextDatum(t_schema);
        values[3] = CStringGetTextDatum(t_table);
        values[4] = CStringGetTextDatum(group);

        int ret = SPI_execute_with_args(
            "INSERT INTO ducklake_sync.table_mappings (group_id, source_schema, source_table, target_schema, target_table) "
            "SELECT id, $1, $2, $3, $4 FROM ducklake_sync.sync_groups WHERE name = $5",
            5, argtypes, values, NULL, false, 0);

        if (ret != SPI_OK_INSERT)
            elog(ERROR, "Failed to add table mapping (group '%s' not found?)", group);
    }

    SPI_finish();
    PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(ducklake_sync_remove_table);
Datum ducklake_sync_remove_table(PG_FUNCTION_ARGS)
{
    char *source_table_arg = text_to_cstring(PG_GETARG_TEXT_PP(0));
    /* bool drop_target = PG_GETARG_BOOL(1); -- unused for now */

    char *source_copy = pstrdup(source_table_arg);
    char *schema = "public";
    char *table = source_copy;
    char *dot = strchr(source_copy, '.');
    if (dot) {
        *dot = '\0';
        schema = source_copy;
        table = dot + 1;
    }

    if (SPI_connect() != SPI_OK_CONNECT) elog(ERROR, "SPI_connect failed");

    /* Use parameterized query to prevent SQL injection */
    {
        Datum values[2];
        Oid argtypes[2] = {TEXTOID, TEXTOID};

        values[0] = CStringGetTextDatum(schema);
        values[1] = CStringGetTextDatum(table);

        int ret = SPI_execute_with_args(
            "DELETE FROM ducklake_sync.table_mappings WHERE source_schema = $1 AND source_table = $2",
            2, argtypes, values, NULL, false, 0);

        if (ret != SPI_OK_DELETE)
            elog(WARNING, "Failed to remove table mapping");
    }

    SPI_finish();
    PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(ducklake_sync_move_table);
Datum ducklake_sync_move_table(PG_FUNCTION_ARGS)
{
    char *source_table_arg = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char *new_group = text_to_cstring(PG_GETARG_TEXT_PP(1));

    char *source_copy = pstrdup(source_table_arg);
    char *schema = "public";
    char *table = source_copy;
    char *dot = strchr(source_copy, '.');
    if (dot) {
        *dot = '\0';
        schema = source_copy;
        table = dot + 1;
    }

    if (SPI_connect() != SPI_OK_CONNECT) elog(ERROR, "SPI_connect failed");

    /* Use parameterized query to prevent SQL injection */
    {
        Datum values[3];
        Oid argtypes[3] = {TEXTOID, TEXTOID, TEXTOID};

        values[0] = CStringGetTextDatum(new_group);
        values[1] = CStringGetTextDatum(schema);
        values[2] = CStringGetTextDatum(table);

        int ret = SPI_execute_with_args(
            "UPDATE ducklake_sync.table_mappings SET group_id = (SELECT id FROM ducklake_sync.sync_groups WHERE name = $1) "
            "WHERE source_schema = $2 AND source_table = $3",
            3, argtypes, values, NULL, false, 0);

        if (ret != SPI_OK_UPDATE)
            elog(ERROR, "Failed to move table");
    }

    SPI_finish();
    PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(ducklake_sync_resync_table);
Datum ducklake_sync_resync_table(PG_FUNCTION_ARGS) { PG_RETURN_VOID(); }

PG_FUNCTION_INFO_V1(ducklake_sync_groups);
Datum ducklake_sync_groups(PG_FUNCTION_ARGS) { return empty_set(fcinfo); }

PG_FUNCTION_INFO_V1(ducklake_sync_tables);
Datum ducklake_sync_tables(PG_FUNCTION_ARGS) { return empty_set(fcinfo); }

PG_FUNCTION_INFO_V1(ducklake_sync_status);
Datum ducklake_sync_status(PG_FUNCTION_ARGS) { return empty_set(fcinfo); }
