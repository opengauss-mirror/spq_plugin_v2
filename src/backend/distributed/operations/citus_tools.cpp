/*-------------------------------------------------------------------------
 *
 * citus_tools.c
 *	  UDF to run multi shard/worker queries
 *
 * This file contains functions to run commands on other worker/shards.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "funcapi.h"
#include "libpq/libpq-fe.h"
#include "knl/knl_variable.h"
#include "miscadmin.h"
#include "executor/exec/execdesc.h"
#include "nodes/nodeFuncs.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"

#include "access/htup.h"
#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"

#include "distributed/backend_data.h"
#include "distributed/bm25_global_stat_cache.h"
#include "distributed/connection_management.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_server_executor.h"
#include "distributed/remote_commands.h"
#include "distributed/utils/array_type.h"
#include "distributed/utils/function.h"
#include "distributed/version_compat.h"
#include "distributed/worker_protocol.h"
#include "distributed/worker_manager.h"
#include "distributed/session_ctx.h"
#include "distributed/citus_custom_scan.h"
#include "distributed/commands.h"
#include "distributed/distributed_planner.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/transaction_management.h"
#include "distributed/commands/utility_hook.h"

PG_FUNCTION_INFO_V1(master_run_on_worker);
extern "C" Datum master_run_on_worker(PG_FUNCTION_ARGS);

static int ParseCommandParameters(FunctionCallInfo fcinfo, StringInfo** nodeNameArray,
                                  int** nodePortsArray, StringInfo** commandStringArray,
                                  bool* parallel);
static void ExecuteCommandsInParallelAndStoreResults(
    StringInfo* nodeNameArray, int* nodePortArray, StringInfo* commandStringArray,
    bool* statusArray, StringInfo* resultStringArray, int commandCount);
static bool GetConnectionStatusAndResult(MultiConnection* connection, bool* resultStatus,
                                         StringInfo queryResultString);
static void ExecuteCommandsAndStoreResults(StringInfo* nodeNameArray, int* nodePortArray,
                                           StringInfo* commandStringArray,
                                           bool* statusArray,
                                           StringInfo* resultStringArray,
                                           int commandCount);
static bool ExecuteOptionalSingleResultCommand(MultiConnection* connection,
                                               char* queryString,
                                               StringInfo queryResultString);
static Tuplestorestate* CreateTupleStore(TupleDesc tupleDescriptor,
                                         StringInfo* nodeNameArray, int* nodePortArray,
                                         bool* statusArray, StringInfo* resultArray,
                                         int commandCount);

/*
 * master_run_on_worker executes queries/commands to run on specified worker and
 * returns success status and query/command result. Expected input is 3 arrays
 * containing node names, node ports, and query strings, and boolean flag to specify
 * parallel execution. The function then returns node_name, node_port, success,
 * result tuples upon completion of the query. The same user credentials are used
 * to connect to remote nodes.
 */
Datum master_run_on_worker(PG_FUNCTION_ARGS)
{
    CheckCitusVersion(ERROR);

    ReturnSetInfo* rsinfo = (ReturnSetInfo*)fcinfo->resultinfo;
    bool parallelExecution = false;
    StringInfo* nodeNameArray = NULL;
    int* nodePortArray = NULL;
    StringInfo* commandStringArray = NULL;

    /* check to see if caller supports us returning a tuplestore */
    if (!rsinfo || !(rsinfo->allowedModes & SFRM_Materialize)) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("materialize mode required, but it is not "
                               "allowed in this context")));
    }

    int commandCount = ParseCommandParameters(fcinfo, &nodeNameArray, &nodePortArray,
                                              &commandStringArray, &parallelExecution);

    MemoryContext per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    MemoryContext oldcontext = MemoryContextSwitchTo(per_query_ctx);

    /* get the requested return tuple description */
    TupleDesc tupleDescriptor = CreateTupleDescCopy(rsinfo->expectedDesc);

    /*
     * Check to make sure we have correct tuple descriptor
     */
    if (tupleDescriptor->natts != 4 ||
        TupleDescAttr(tupleDescriptor, 0)->atttypid != TEXTOID ||
        TupleDescAttr(tupleDescriptor, 1)->atttypid != INT4OID ||
        TupleDescAttr(tupleDescriptor, 2)->atttypid != BOOLOID ||
        TupleDescAttr(tupleDescriptor, 3)->atttypid != TEXTOID) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_COLUMN_DEFINITION),
                        errmsg("query-specified return tuple and "
                               "function return type are not compatible")));
    }

    /*
     * prepare storage for status and result values.
     * commandCount is based on user input however, it is the length of list
     * instead of a user given integer, hence this should be safe here in terms
     * of memory allocation.
     */
    bool* statusArray = static_cast<bool*>(palloc0(commandCount * sizeof(bool)));
    StringInfo* resultArray =
        static_cast<StringInfo*>(palloc0(commandCount * sizeof(StringInfo)));
    for (int commandIndex = 0; commandIndex < commandCount; commandIndex++) {
        resultArray[commandIndex] = makeStringInfo();
    }

    if (parallelExecution) {
        ExecuteCommandsInParallelAndStoreResults(nodeNameArray, nodePortArray,
                                                 commandStringArray, statusArray,
                                                 resultArray, commandCount);
    } else {
        ExecuteCommandsAndStoreResults(nodeNameArray, nodePortArray, commandStringArray,
                                       statusArray, resultArray, commandCount);
    }

    /* let the caller know we're sending back a tuplestore */
    rsinfo->returnMode = SFRM_Materialize;
    Tuplestorestate* tupleStore =
        CreateTupleStore(tupleDescriptor, nodeNameArray, nodePortArray, statusArray,
                         resultArray, commandCount);
    rsinfo->setResult = tupleStore;
    rsinfo->setDesc = tupleDescriptor;

    MemoryContextSwitchTo(oldcontext);

    PG_RETURN_VOID();
}

/* ParseCommandParameters reads call parameters and fills in data structures */
static int ParseCommandParameters(FunctionCallInfo fcinfo, StringInfo** nodeNameArray,
                                  int** nodePortsArray, StringInfo** commandStringArray,
                                  bool* parallel)
{
    ArrayType* nodeNameArrayObject = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType* nodePortArrayObject = PG_GETARG_ARRAYTYPE_P(1);
    ArrayType* commandStringArrayObject = PG_GETARG_ARRAYTYPE_P(2);
    bool parallelExecution = PG_GETARG_BOOL(3);
    int nodeNameCount = ArrayObjectCount(nodeNameArrayObject);
    int nodePortCount = ArrayObjectCount(nodePortArrayObject);
    int commandStringCount = ArrayObjectCount(commandStringArrayObject);
    Datum* nodeNameDatumArray = DeconstructArrayObject(nodeNameArrayObject);
    Datum* nodePortDatumArray = DeconstructArrayObject(nodePortArrayObject);
    Datum* commandStringDatumArray = DeconstructArrayObject(commandStringArrayObject);

    if (nodeNameCount != nodePortCount || nodeNameCount != commandStringCount) {
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("expected same number of node name, port, and query string")));
    }

    StringInfo* nodeNames =
        static_cast<StringInfo*>(palloc0(nodeNameCount * sizeof(StringInfo)));
    int* nodePorts = static_cast<int*>(palloc0(nodeNameCount * sizeof(int)));
    StringInfo* commandStrings =
        static_cast<StringInfo*>(palloc0(nodeNameCount * sizeof(StringInfo)));

    for (int index = 0; index < nodeNameCount; index++) {
        text* nodeNameText = DatumGetTextP(nodeNameDatumArray[index]);
        char* nodeName = text_to_cstring(nodeNameText);
        int32 nodePort = DatumGetInt32(nodePortDatumArray[index]);
        text* commandText = DatumGetTextP(commandStringDatumArray[index]);
        char* commandString = text_to_cstring(commandText);

        nodeNames[index] = makeStringInfo();
        commandStrings[index] = makeStringInfo();

        appendStringInfo(nodeNames[index], "%s", nodeName);
        nodePorts[index] = nodePort;
        appendStringInfo(commandStrings[index], "%s", commandString);
    }

    *nodeNameArray = nodeNames;
    *nodePortsArray = nodePorts;
    *commandStringArray = commandStrings;
    *parallel = parallelExecution;

    return nodeNameCount;
}

/*
 * ExecuteCommandsInParallelAndStoreResults connects to each node specified in
 * nodeNameArray and nodePortArray, and executes command in commandStringArray
 * in parallel fashion. Execution success status and result is reported for
 * each command in statusArray and resultStringArray. Each array contains
 * commandCount items.
 */
static void ExecuteCommandsInParallelAndStoreResults(
    StringInfo* nodeNameArray, int* nodePortArray, StringInfo* commandStringArray,
    bool* statusArray, StringInfo* resultStringArray, int commandCount)
{
    MultiConnection** connectionArray =
        static_cast<MultiConnection**>(palloc0(commandCount * sizeof(MultiConnection*)));
    int finishedCount = 0;

    /* start connections asynchronously */
    for (int commandIndex = 0; commandIndex < commandCount; commandIndex++) {
        char* nodeName = nodeNameArray[commandIndex]->data;
        int nodePort = nodePortArray[commandIndex];
        int connectionFlags = FORCE_NEW_CONNECTION;
        connectionArray[commandIndex] =
            StartNodeConnection(connectionFlags, nodeName, nodePort);
    }

    /* establish connections */
    for (int commandIndex = 0; commandIndex < commandCount; commandIndex++) {
        MultiConnection* connection = connectionArray[commandIndex];
        StringInfo queryResultString = resultStringArray[commandIndex];
        char* nodeName = nodeNameArray[commandIndex]->data;
        int nodePort = nodePortArray[commandIndex];

        FinishConnectionEstablishment(connection);

        /* check whether connection attempt was successful */
        if (PQstatus(connection->pgConn) != CONNECTION_OK) {
            appendStringInfo(queryResultString, "failed to connect to %s:%d", nodeName,
                             nodePort);
            statusArray[commandIndex] = false;
            CloseConnection(connection);
            connectionArray[commandIndex] = NULL;
            finishedCount++;
            continue;
        }

        /* set the application_name to avoid nested execution checks */
        int querySent = SendRemoteCommand(
            connection,
            psprintf("SET application_name TO '%s%ld'",
                     SPQ_RUN_COMMAND_APPLICATION_NAME_PREFIX, GetGlobalPID()));
        if (querySent == 0) {
            StoreErrorMessage(connection, queryResultString);
            statusArray[commandIndex] = false;
            CloseConnection(connection);
            connectionArray[commandIndex] = NULL;
            finishedCount++;
            continue;
        }

        statusArray[commandIndex] = true;
    }

    /* send queries at once */
    for (int commandIndex = 0; commandIndex < commandCount; commandIndex++) {
        MultiConnection* connection = connectionArray[commandIndex];
        if (connection == NULL) {
            continue;
        }

        bool raiseInterrupts = true;
        PGresult* queryResult = GetRemoteCommandResult(connection, raiseInterrupts);

        /* write the result value or error message to queryResultString */
        StringInfo queryResultString = resultStringArray[commandIndex];
        bool success =
            EvaluateSingleQueryResult(connection, queryResult, queryResultString);
        if (!success) {
            statusArray[commandIndex] = false;
            CloseConnection(connection);
            connectionArray[commandIndex] = NULL;
            finishedCount++;
            continue;
        }

        /* clear results for the next command */
        PQclear(queryResult);

        bool raiseErrors = false;
        ClearResults(connection, raiseErrors);

        /* we only care about the SET application_name result on failure */
        resetStringInfo(queryResultString);
    }

    /* send queries at once */
    for (int commandIndex = 0; commandIndex < commandCount; commandIndex++) {
        MultiConnection* connection = connectionArray[commandIndex];
        char* queryString = commandStringArray[commandIndex]->data;
        StringInfo queryResultString = resultStringArray[commandIndex];

        /*
         * If we don't have a connection, nothing to send, error string should already
         * been filled.
         */
        if (connection == NULL) {
            continue;
        }

        int querySent = SendRemoteCommand(connection, queryString);
        if (querySent == 0) {
            StoreErrorMessage(connection, queryResultString);
            statusArray[commandIndex] = false;
            CloseConnection(connection);
            connectionArray[commandIndex] = NULL;
            finishedCount++;
        }
    }

    /* check for query results */
    while (finishedCount < commandCount) {
        for (int commandIndex = 0; commandIndex < commandCount; commandIndex++) {
            MultiConnection* connection = connectionArray[commandIndex];
            StringInfo queryResultString = resultStringArray[commandIndex];
            bool success = false;

            if (connection == NULL) {
                continue;
            }

            bool queryFinished =
                GetConnectionStatusAndResult(connection, &success, queryResultString);

            if (queryFinished) {
                finishedCount++;
                statusArray[commandIndex] = success;
                connectionArray[commandIndex] = NULL;
                CloseConnection(connection);
            }
        }

        CHECK_FOR_INTERRUPTS();

        if (finishedCount < commandCount) {
            long sleepIntervalPerCycle =
                Session_ctx::Vars().RemoteTaskCheckInterval * 1000L;
            pg_usleep(sleepIntervalPerCycle);
        }
    }

    pfree(connectionArray);
}

/*
 * GetConnectionStatusAndResult checks the active connection and returns true if
 * query execution is finished (either success or fail).
 * Query success/fail in resultStatus, and query result in queryResultString are
 * reported upon completion of the query.
 */
static bool GetConnectionStatusAndResult(MultiConnection* connection, bool* resultStatus,
                                         StringInfo queryResultString)
{
    bool finished = true;
    ConnStatusType connectionStatus = PQstatus(connection->pgConn);

    *resultStatus = false;
    resetStringInfo(queryResultString);

    if (connectionStatus == CONNECTION_BAD) {
        appendStringInfo(queryResultString, "connection lost");
        return finished;
    }

    int consumeInput = PQconsumeInput(connection->pgConn);
    if (consumeInput == 0) {
        appendStringInfo(queryResultString, "query result unavailable");
        return finished;
    }

    /* check later if busy */
    if (PQisBusy(connection->pgConn) != 0) {
        finished = false;
        return finished;
    }

    /* query result is available at this point */
    PGresult* queryResult = PQgetResult(connection->pgConn);
    bool success = EvaluateSingleQueryResult(connection, queryResult, queryResultString);
    PQclear(queryResult);

    *resultStatus = success;
    finished = true;
    return true;
}

/*
 * ExecuteCommandsAndStoreResults connects to each node specified in
 * nodeNameArray and nodePortArray, and executes command in commandStringArray
 * in sequential order. Execution success status and result is reported for
 * each command in statusArray and resultStringArray. Each array contains
 * commandCount items.
 */
static void ExecuteCommandsAndStoreResults(StringInfo* nodeNameArray, int* nodePortArray,
                                           StringInfo* commandStringArray,
                                           bool* statusArray,
                                           StringInfo* resultStringArray,
                                           int commandCount)
{
    for (int commandIndex = 0; commandIndex < commandCount; commandIndex++) {
        CHECK_FOR_INTERRUPTS();

        char* nodeName = nodeNameArray[commandIndex]->data;
        int32 nodePort = nodePortArray[commandIndex];
        char* queryString = commandStringArray[commandIndex]->data;
        StringInfo queryResultString = resultStringArray[commandIndex];

        int connectionFlags = FORCE_NEW_CONNECTION;
        MultiConnection* connection =
            GetNodeConnection(connectionFlags, nodeName, nodePort);

        /* set the application_name to avoid nested execution checks */
        bool success = ExecuteOptionalSingleResultCommand(
            connection,
            psprintf("SET application_name TO '%s%ld'",
                     SPQ_RUN_COMMAND_APPLICATION_NAME_PREFIX, GetGlobalPID()),
            queryResultString);
        if (!success) {
            statusArray[commandIndex] = false;
            CloseConnection(connection);
            continue;
        }

        /* we only care about the SET application_name result on failure */
        resetStringInfo(queryResultString);

        /* send the actual query string */
        success = ExecuteOptionalSingleResultCommand(connection, queryString,
                                                     queryResultString);

        statusArray[commandIndex] = success;
        CloseConnection(connection);
    }
}

/*
 * ExecuteOptionalSingleResultCommand executes a query at specified remote node using
 * the calling user's credentials. The function returns the query status
 * (success/failure), and query result. The query is expected to return a single
 * target containing zero or one rows.
 */
static bool ExecuteOptionalSingleResultCommand(MultiConnection* connection,
                                               char* queryString,
                                               StringInfo queryResultString)
{
    if (PQstatus(connection->pgConn) != CONNECTION_OK) {
        appendStringInfo(queryResultString, "failed to connect to %s:%d",
                         connection->hostname, connection->port);
        return false;
    }

    if (!SendRemoteCommand(connection, queryString)) {
        appendStringInfo(queryResultString, "failed to send query to %s:%d",
                         connection->hostname, connection->port);
        return false;
    }

    bool raiseInterrupts = true;
    PGresult* queryResult = GetRemoteCommandResult(connection, raiseInterrupts);

    /* write the result value or error message to queryResultString */
    bool success = EvaluateSingleQueryResult(connection, queryResult, queryResultString);

    /* clear result and close the connection */
    PQclear(queryResult);

    bool raiseErrors = false;
    ClearResults(connection, raiseErrors);

    return success;
}

/* CreateTupleStore prepares result tuples from individual query results */
static Tuplestorestate* CreateTupleStore(TupleDesc tupleDescriptor,
                                         StringInfo* nodeNameArray, int* nodePortArray,
                                         bool* statusArray, StringInfo* resultArray,
                                         int commandCount)
{
    Tuplestorestate* tupleStore =
        tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    bool nulls[4] = {false, false, false, false};

    for (int commandIndex = 0; commandIndex < commandCount; commandIndex++) {
        Datum values[4];
        StringInfo nodeNameString = nodeNameArray[commandIndex];
        StringInfo resultString = resultArray[commandIndex];
        text* nodeNameText =
            cstring_to_text_with_len(nodeNameString->data, nodeNameString->len);
        text* resultText =
            cstring_to_text_with_len(resultString->data, resultString->len);

        values[0] = PointerGetDatum(nodeNameText);
        values[1] = Int32GetDatum(nodePortArray[commandIndex]);
        values[2] = BoolGetDatum(statusArray[commandIndex]);
        values[3] = PointerGetDatum(resultText);

        HeapTuple tuple = heap_form_tuple(tupleDescriptor, values, nulls);
        tuplestore_puttuple(tupleStore, tuple);

        heap_freetuple(tuple);
        pfree(nodeNameText);
        pfree(resultText);
    }
    return tupleStore;
}

PG_FUNCTION_INFO_V1(bm25_collect_global_stat);
extern "C" Datum bm25_collect_global_stat(PG_FUNCTION_ARGS);

/*
 * Term buffer must hold any token the kernel can produce, otherwise long terms get
 * truncated here and their df is attributed to the wrong term. Keep in sync with
 * BM25_MAX_TOKEN_LEN in access/datavec/bm25.h (the kernel truncates to that size).
 */
#define BM25_GLOBAL_DF_TERM_LEN 100

/* Per query-term global document frequency accumulator. */
typedef struct Bm25DfEntry {
    char term[BM25_GLOBAL_DF_TERM_LEN];
    uint64 df;
} Bm25DfEntry;

static THR_LOCAL char* Bm25QueryPrefix = NULL;

static char* Bm25ShardTableRegex(const char* tableName)
{
    StringInfoData regex;
    initStringInfo(&regex);
    appendStringInfoChar(&regex, '^');
    for (const char* p = tableName; *p != '\0'; p++) {
        if (strchr(".^$*+?()[]{}|\\", *p) != NULL) {
            appendStringInfoChar(&regex, '\\');
        }
        appendStringInfoChar(&regex, *p);
    }
    appendStringInfoString(&regex, "_[0-9]+$");
    return regex.data;
}

static bool Bm25ParseUnsigned(const char* value, uint64* parsed)
{
    if (value == NULL || *value == '\0') {
        return false;
    }
    errno = 0;
    char* end = NULL;
    unsigned long long number = strtoull(value, &end, 10);
    if (errno == ERANGE || end == value || *end != '\0') {
        return false;
    }
    *parsed = (uint64)number;
    return true;
}

static void Bm25RemoveZeroDfEntries(Bm25DfEntry* entries, int* count)
{
    int writeIndex = 0;
    for (int readIndex = 0; readIndex < *count; readIndex++) {
        if (entries[readIndex].df == 0) {
            continue;
        }
        if (writeIndex != readIndex) {
            entries[writeIndex] = entries[readIndex];
        }
        writeIndex++;
    }
    *count = writeIndex;
}

static void Bm25AccumulateDf(Bm25DfEntry** entries, int* count, int* capacity,
                             const char* term, uint64 df)
{
    for (int i = 0; i < *count; i++) {
        if (strcmp((*entries)[i].term, term) == 0) {
            (*entries)[i].df += df;
            return;
        }
    }
    if (*count >= *capacity) {
        *capacity *= 2;
        *entries = (Bm25DfEntry*)repalloc(*entries, *capacity * sizeof(Bm25DfEntry));
    }
    errno_t rc = strncpy_s((*entries)[*count].term, sizeof((*entries)[*count].term), term,
                           sizeof((*entries)[*count].term) - 1);
    securec_check(rc, "\0", "\0");
    (*entries)[*count].df = df;
    (*count)++;
}

/*
 * Parse one shard's bm25_shard_stat payload "N=<n>;T=<t>;term:df,term:df,..."
 * and accumulate into the global totals.
 */
static bool Bm25ParseShardStat(char* shard, uint64* totalN, uint64* totalT,
                               Bm25DfEntry** dfEntries, int* dfCount, int* dfCapacity)
{
    bool sawN = false;
    bool sawT = false;
    char* partCtx = NULL;
    for (char* part = strtok_r(shard, ";", &partCtx); part != NULL;
         part = strtok_r(NULL, ";", &partCtx)) {
        while (*part == ' ') {
            part++;
        }
        if (strncmp(part, "N=", 2) == 0) {
            uint64 value = 0;
            if (!Bm25ParseUnsigned(part + 2, &value)) {
                return false;
            }
            *totalN += value;
            sawN = true;
        } else if (strncmp(part, "T=", 2) == 0) {
            uint64 value = 0;
            if (!Bm25ParseUnsigned(part + 2, &value)) {
                return false;
            }
            *totalT += value;
            sawT = true;
        } else if (strchr(part, ':') != NULL) {
            char* kvCtx = NULL;
            for (char* kv = strtok_r(part, ",", &kvCtx); kv != NULL;
                 kv = strtok_r(NULL, ",", &kvCtx)) {
                char* colon = strchr(kv, ':');
                if (colon == NULL) {
                    continue;
                }
                *colon = '\0';
                uint64 value = 0;
                if (*kv == '\0' || !Bm25ParseUnsigned(colon + 1, &value)) {
                    return false;
                }
                Bm25AccumulateDf(dfEntries, dfCount, dfCapacity, kv, value);
            }
        }
    }
    return sawN && sawT;
}

static void Bm25SetQueryPrefix(const char* stat)
{
    if (Bm25QueryPrefix != NULL) {
        pfree(Bm25QueryPrefix);
    }
    StringInfo prefix = makeStringInfo();
    appendStringInfo(prefix,
                     "SET LOCAL enable_bm25_global_idf = on;"
                     "SET LOCAL bm25_global_stat = %s;",
                     quote_literal_cstr(stat));
    Bm25QueryPrefix = prefix->data;
    pfree(prefix);
}

static void Bm25SetLocalGuc(const char* name, const char* value)
{
    (void)set_config_option(name, value, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_LOCAL,
                            true, 0);

    StringInfoData command;
    initStringInfo(&command);
    appendStringInfo(&command, "SET LOCAL %s = %s;", name, quote_literal_cstr(value));

    VariableSetStmt* setStmt = makeNode(VariableSetStmt);
    setStmt->kind = VAR_SET_VALUE;
    setStmt->name = (char*)name;
    setStmt->is_local = true;
    PostprocessVariableSetStmt(setStmt, command.data);

    pfree(setStmt);
    pfree(command.data);
}

static bool Bm25QueryPrefixIsSafe(void)
{
    return !IsMultiStatementTransaction() && !InCoordinatedTransaction();
}

void ResetBm25GlobalStatQueryPrefix(void)
{
    pfree_ext(Bm25QueryPrefix);
    Bm25QueryPrefix = NULL;
}

const char* GetBm25GlobalStatQueryPrefix(void)
{
    return Bm25QueryPrefix;
}

/*
 * Bm25CollectGlobalStatInternal collects distributed BM25 global stats over SPQ's
 * cached worker connections (zero connection-establishment overhead): in parallel run
 * bm25_shard_stat() over every BM25 shard index on each DN, aggregate N / T / per-term
 * df, and prepare the totals for query-scoped SET LOCAL injection.
 *
 * The SET LOCAL prefix is sent in the same simple-protocol command as each shard query.
 * This keeps the statistics and query in one implicit worker transaction while avoiding
 * a separate BEGIN/COMMIT cycle and the activeSetStmts replay path.
 *
 * tableName limits collection to one distributed table's shards (shard relations are
 * named <table>_<shardid> on the DNs); NULL aggregates every BM25 shard index, which
 * is only correct when a single BM25-indexed table exists.
 *
 * Returns false (leaving GUCs untouched) when no shard reported documents.
 */
static bool Bm25CollectGlobalStatInternal(const char* queryStr, const char* tableName,
                                          StringInfo summary, StringInfo statOut)
{
    List* workerNodes = ActivePrimaryNodeList(NoLock);
    int workerCount = list_length(workerNodes);
    if (workerCount == 0) {
        ereport(ERROR, (errmsg("bm25_collect_global_stat: no active worker nodes")));
    }

    MultiConnection** conns =
        (MultiConnection**)palloc0(workerCount * sizeof(MultiConnection*));
    char** nodeNames = (char**)palloc0(workerCount * sizeof(char*));
    int* nodePorts = (int*)palloc0(workerCount * sizeof(int));
    bool* commandSent = (bool*)palloc0(workerCount * sizeof(bool));
    int connCount = 0;

    ListCell* cell = NULL;
    foreach (cell, workerNodes) {
        WorkerNode* node = (WorkerNode*)lfirst(cell);
        MultiConnection* conn = GetNodeConnection(0, node->workerName, node->workerPort);
        if (conn != NULL && PQstatus(conn->pgConn) == CONNECTION_OK) {
            conns[connCount] = conn;
            nodeNames[connCount] = node->workerName;
            nodePorts[connCount] = node->workerPort;
            connCount++;
        }
    }
    if (connCount == 0) {
        ereport(ERROR, (errmsg("bm25_collect_global_stat: no connections available")));
    }
    /*
     * A node we cannot reach contributes neither statistics nor rows, so the collected
     * stats stay consistent with the data the query can actually see. Say so anyway: the
     * distributed query itself is about to fail on that node.
     */
    if (connCount < workerCount) {
        ereport(WARNING, (errmsg("bm25_collect_global_stat: %d of %d nodes unreachable, "
                                 "global statistics are incomplete",
                                 workerCount - connCount, workerCount)));
    }

    /*
     * Without a table name we cannot tell this table's shard indexes from those of any
     * other BM25 table on the node, and summing across tables would produce wrong IDF.
     * Keeping local IDF is the safe answer.
     */
    if (tableName == NULL) {
        ereport(DEBUG1,
                (errmsg("bm25_collect_global_stat: no target table, keeping local IDF")));
        pfree(conns);
        pfree(nodeNames);
        pfree(nodePorts);
        pfree(commandSent);
        return false;
    }

    /* Use bm25_table_stat: tokenizes once, aggregates all shard indexes on each DN. */
    StringInfoData sql;
    initStringInfo(&sql);
    char* shardRegex = Bm25ShardTableRegex(tableName);
    appendStringInfo(&sql, "SELECT pg_catalog.bm25_table_stat(%s, %s)",
                     quote_literal_cstr(shardRegex), quote_literal_cstr(queryStr));
    pfree(shardRegex);

    /* send to all DNs first, then reap: effectively parallel on cached connections */
    bool statsComplete = (connCount == workerCount);
    for (int i = 0; i < connCount; i++) {
        commandSent[i] = SendRemoteCommand(conns[i], sql.data) != 0;
        if (!commandSent[i]) {
            statsComplete = false;
            ereport(WARNING, (errmsg("bm25_collect_global_stat: send failed to %s:%d",
                                     nodeNames[i], nodePorts[i])));
        }
    }

    uint64 totalN = 0;
    uint64 totalT = 0;
    int dfCapacity = 32;
    int dfCount = 0;
    Bm25DfEntry* dfEntries = (Bm25DfEntry*)palloc0(dfCapacity * sizeof(Bm25DfEntry));

    for (int i = 0; i < connCount; i++) {
        if (!commandSent[i]) {
            continue;
        }
        PGresult* result = GetRemoteCommandResult(conns[i], true);
        if (result != NULL && PQresultStatus(result) == PGRES_TUPLES_OK) {
            for (int r = 0; r < PQntuples(result); r++) {
                if (PQgetisnull(result, r, 0)) {
                    statsComplete = false;
                    continue;
                }
                char* val = pstrdup(PQgetvalue(result, r, 0));
                if (!Bm25ParseShardStat(val, &totalN, &totalT, &dfEntries, &dfCount,
                                        &dfCapacity)) {
                    statsComplete = false;
                    ereport(
                        WARNING,
                        (errmsg("bm25_collect_global_stat: invalid statistics from %s:%d",
                                nodeNames[i], nodePorts[i])));
                }
                pfree(val);
            }
        } else {
            statsComplete = false;
            ereport(WARNING, (errmsg("bm25_collect_global_stat: error from %s:%d: %s",
                                     nodeNames[i], nodePorts[i],
                                     result == NULL ? "no result"
                                                    : PQresultErrorMessage(result))));
        }
        if (result != NULL) {
            PQclear(result);
        }
        ClearResults(conns[i], false);
    }

    if (!statsComplete || totalN == 0) {
        pfree(dfEntries);
        pfree(sql.data);
        pfree(conns);
        pfree(nodeNames);
        pfree(nodePorts);
        pfree(commandSent);
        return false;
    }

    /*
     * A term absent from every shard has no usable global df. The kernel's atomic
     * payload intentionally requires df >= 1, so omit those terms and let scans use
     * their normal local fallback for them.
     */
    Bm25RemoveZeroDfEntries(dfEntries, &dfCount);

    StringInfoData dfStr;
    initStringInfo(&dfStr);
    for (int i = 0; i < dfCount; i++) {
        appendStringInfo(&dfStr, "%s%s:%lu", (i > 0 ? "," : ""), dfEntries[i].term,
                         (unsigned long)dfEntries[i].df);
    }

    StringInfoData stat;
    initStringInfo(&stat);
    appendStringInfo(&stat, "N=%lu;T=%lu;%s", (unsigned long)totalN,
                     (unsigned long)totalT, dfStr.data);

    if (summary != NULL) {
        appendStringInfo(summary, "N=%lu T=%lu df=%s (workers=%d)", (unsigned long)totalN,
                         (unsigned long)totalT, dfStr.data, connCount);
    }
    if (statOut != NULL) {
        appendStringInfoString(statOut, stat.data);
    }

    pfree(stat.data);
    pfree(dfStr.data);
    pfree(dfEntries);
    pfree(sql.data);
    pfree(conns);
    pfree(nodeNames);
    pfree(nodePorts);
    pfree(commandSent);
    return true;
}

/*
 * bm25_collect_global_stat(query_text text [, table_name text])
 *
 * SQL entry of Bm25CollectGlobalStatInternal for manual/debug use; the transparent
 * path is TryCollectBm25GlobalStat called from CitusExecutorStart.
 */
Datum bm25_collect_global_stat(PG_FUNCTION_ARGS)
{
    CheckCitusVersion(ERROR);

    if (PG_ARGISNULL(0)) {
        ereport(ERROR, (errmsg("bm25_collect_global_stat: query_text must not be NULL")));
    }
    char* queryStr = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char* tableName = (PG_NARGS() > 1 && !PG_ARGISNULL(1))
                          ? text_to_cstring(PG_GETARG_TEXT_PP(1))
                          : NULL;

    StringInfoData summary;
    StringInfoData stat;
    initStringInfo(&summary);
    initStringInfo(&stat);
    if (!Bm25CollectGlobalStatInternal(queryStr, tableName, &summary, &stat)) {
        pfree(queryStr);
        pfree_ext(tableName);
        pfree(stat.data);
        PG_RETURN_TEXT_P(cstring_to_text("no stats"));
    }

    if (Session_ctx::Vars().PropagateSetCommands != PROPSETCMD_LOCAL) {
        (void)set_config_option("spq.propagate_set_commands", "local", PGC_USERSET,
                                PGC_S_SESSION, GUC_ACTION_LOCAL, true, 0);
    }
    UseCoordinatedTransaction();
    Bm25SetLocalGuc("enable_bm25_global_idf", "on");
    Bm25SetLocalGuc("bm25_global_stat", stat.data);

    pfree(queryStr);
    pfree_ext(tableName);
    pfree(stat.data);
    PG_RETURN_TEXT_P(cstring_to_text(summary.data));
}

/* <&> BM25 ordering operator OID (kernel builtin, see pg_operator) */
#define BM25_ORDER_BY_OP_OID 6208

/*
 * Bm25QueryTextWalker finds a BM25 <&> OpExpr with a constant text argument anywhere
 * in a (worker job) query tree and extracts the search text. Parameterized query
 * texts are not extractable here, so those queries simply keep local IDF.
 */
static bool Bm25QueryTextWalker(Node* node, char** queryTextOut)
{
    if (node == NULL) {
        return false;
    }
    if (IsA(node, OpExpr)) {
        OpExpr* op = (OpExpr*)node;
        if (op->opno == BM25_ORDER_BY_OP_OID && list_length(op->args) == 2) {
            Node* left = (Node*)linitial(op->args);
            Node* right = (Node*)lsecond(op->args);
            Const* con = NULL;
            if (IsA(right, Const)) {
                con = (Const*)right;
            } else if (IsA(left, Const)) {
                con = (Const*)left;
            }
            if (con != NULL && !con->constisnull && con->consttype == TEXTOID) {
                *queryTextOut = text_to_cstring(DatumGetTextPP(con->constvalue));
                return true;
            }
        }
    }
    if (IsA(node, Query)) {
        return query_tree_walker((Query*)node,
                                 reinterpret_cast<bool (*)()>(Bm25QueryTextWalker),
                                 queryTextOut, 0);
    }
    return expression_tree_walker(node, reinterpret_cast<bool (*)()>(Bm25QueryTextWalker),
                                  queryTextOut);
}

/*
 * FindSpqDistributedPlan walks a plan tree and returns the DistributedPlan carried by
 * the first SPQ ExtensiblePlan node, or NULL. (IsCitusCustomScan/FetchCitusCustomScan-
 * IfExists check IsA(plan, Scan), which never matches T_ExtensiblePlan on openGauss,
 * so they cannot be used here.)
 */
static DistributedPlan* FindSpqDistributedPlan(Plan* plan)
{
    if (plan == NULL) {
        return NULL;
    }
    if (IsA(plan, ExtensiblePlan)) {
        ExtensiblePlan* customScan = (ExtensiblePlan*)plan;
        if (list_length(customScan->extensible_private) > 0) {
            Node* privateNode = (Node*)linitial(customScan->extensible_private);
            if (CitusIsA(privateNode, DistributedPlan)) {
                return (DistributedPlan*)privateNode;
            }
        }
        return NULL;
    }
    DistributedPlan* found = FindSpqDistributedPlan(plan->lefttree);
    if (found == NULL) {
        found = FindSpqDistributedPlan(plan->righttree);
    }
    return found;
}

/*
 * TryCollectBm25GlobalStat makes distributed BM25 global IDF transparent: called
 * from CitusExecutorStart, it detects a BM25 <&> ordering inside the distributed
 * plan's worker job query, collects the cluster-wide stats and injects them as
 * SET LOCAL GUCs before the plan is shipped, so one plain SQL statement suffices.
 *
 * Statistics are cached by database, logical relation and query text. Fresh hits avoid
 * all DN traffic. On a miss or expiry one backend refreshes without holding the cache
 * lock; concurrent backends do not wait and keep local IDF for that statement.
 */
void TryCollectBm25GlobalStat(QueryDesc* queryDesc)
{
    if (!u_sess->attr.attr_sql.enable_bm25_global_idf) {
        return;
    }
    if (!Bm25QueryPrefixIsSafe()) {
        ereport(DEBUG1,
                (errmsg("bm25 auto-collect: local IDF fallback inside an explicit or "
                        "coordinated transaction")));
        return;
    }
    if (queryDesc == NULL || queryDesc->plannedstmt == NULL) {
        return;
    }

    DistributedPlan* distributedPlan =
        FindSpqDistributedPlan(queryDesc->plannedstmt->planTree);
    if (distributedPlan == NULL) {
        ereport(DEBUG1, (errmsg("bm25 auto-collect: no distributed plan found")));
        return;
    }
    if (distributedPlan->workerJob == NULL ||
        distributedPlan->workerJob->jobQuery == NULL) {
        ereport(DEBUG1, (errmsg("bm25 auto-collect: workerJob=%p jobQuery missing",
                                (void*)distributedPlan->workerJob)));
        return;
    }

    char* queryText = NULL;
    (void)query_tree_walker(distributedPlan->workerJob->jobQuery,
                            reinterpret_cast<bool (*)()>(Bm25QueryTextWalker), &queryText,
                            0);
    if (queryText == NULL) {
        ereport(DEBUG1, (errmsg("bm25 auto-collect: no <&> const found in job query")));
        return;
    }

    /* limit collection to the queried table's shards when the plan tells us */
    char* tableName = NULL;
    Oid relationOid = InvalidOid;
    if (list_length(distributedPlan->relationIdList) == 1) {
        relationOid = linitial_oid(distributedPlan->relationIdList);
        tableName = get_rel_name(relationOid);
    }
    if (!OidIsValid(relationOid) || tableName == NULL) {
        pfree(queryText);
        return;
    }

    char* cachedStat = NULL;
    Bm25GlobalStatCacheResult cacheResult = Bm25GlobalStatCacheLookup(
        u_sess->proc_cxt.MyDatabaseId, relationOid, queryText, &cachedStat);
    if (cacheResult == BM25_GLOBAL_STAT_CACHE_HIT) {
        Bm25SetQueryPrefix(cachedStat);
        pfree(cachedStat);
        pfree(queryText);
        return;
    }
    if (cacheResult == BM25_GLOBAL_STAT_CACHE_FALLBACK) {
        pfree(queryText);
        return;
    }

    ereport(DEBUG1,
            (errmsg("bm25 auto-collect: collecting for query text \"%s\" (table %s)",
                    queryText, tableName ? tableName : "<all>")));
    StringInfoData stat;
    initStringInfo(&stat);
    PG_TRY();
    {
        if (Bm25CollectGlobalStatInternal(queryText, tableName, NULL, &stat)) {
            Bm25SetQueryPrefix(stat.data);
            if (cacheResult == BM25_GLOBAL_STAT_CACHE_REFRESH) {
                Bm25GlobalStatCacheStore(u_sess->proc_cxt.MyDatabaseId, relationOid,
                                         queryText, stat.data);
            }
        } else {
            if (cacheResult == BM25_GLOBAL_STAT_CACHE_REFRESH) {
                Bm25GlobalStatCacheAbortRefresh(u_sess->proc_cxt.MyDatabaseId,
                                                relationOid, queryText);
            }
        }
    }
    PG_CATCH();
    {
        if (cacheResult == BM25_GLOBAL_STAT_CACHE_REFRESH) {
            Bm25GlobalStatCacheAbortRefresh(u_sess->proc_cxt.MyDatabaseId, relationOid,
                                            queryText);
        }
        pfree(stat.data);
        pfree(queryText);
        PG_RE_THROW();
    }
    PG_END_TRY();
    pfree(stat.data);
    pfree(queryText);
}
