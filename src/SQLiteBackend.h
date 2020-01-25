#ifndef BACKEND_H
#define BACKEND_H

#include <time.h>
#include <stdlib.h>
#include "open62541.h"
#include "sqlite3.h"

//#undef EOF

static const size_t END_OF_DATA = SIZE_MAX;
static const size_t QUERY_BUFFER_SIZE = 500;

#define WINDOWS_TICK 10000000
#define SEC_TO_UNIX_EPOCH 11644473600LL


size_t OpcTimestampToUnixSeconds(UA_DateTime timestamp)
{
	return (size_t)(timestamp / WINDOWS_TICK - SEC_TO_UNIX_EPOCH);
}


UA_DateTime UnixSecondsToOpcTimestamp(size_t unixSeconds)
{
	return (unixSeconds + SEC_TO_UNIX_EPOCH) * WINDOWS_TICK;
}


size_t convertTimestampStringToUnixSeconds(const char* timestampString)
{
	struct tm tm;
	tm.tm_isdst = -1;

	sscanf_s(timestampString, "%d-%d-%d %d:%d:%d", &tm.tm_year, &tm.tm_mon, &tm.tm_mday, &tm.tm_hour, &tm.tm_min, &tm.tm_sec);

	tm.tm_mon -= 1;
	tm.tm_year -= 1900;

	//Todo: add conditional for Linux
	time_t t = _mkgmtime(&tm);

	return t;
}


const char* convertUnixSecondsToTimestampString(size_t unixSeconds)
{
	static char buffer[20];

	time_t ts = unixSeconds;

	struct tm tm;

	gmtime_s(&tm, &ts);

	int s = sizeof(buffer);

	strftime(buffer, 20, "%Y-%m-%d %H:%M:%S", &tm);

	return buffer;
}



//Context that is needed for the SQLite callback for copying data.
struct context_copyDataValues {
	size_t maxValues;
	size_t counter;
	UA_DataValue *values;
}; 


struct context_sqlite {

	sqlite3* sqlite;

	const char* measuringPointID;
};


static struct context_sqlite*
generateContext_sqlite(const char* filename) {
	
	sqlite3* handle;
	
	int res = sqlite3_open(filename, &handle);
	
	if (res != SQLITE_OK)
		return NULL;

	struct context_sqlite* ret = (struct context_sqlite*)UA_calloc(1, sizeof(struct context_sqlite));

	ret->sqlite = handle;

	//For this demo we have only one source measuring point which we hardcode in the context.
	//A more advanced demo should determine the available measuring points from the source
	//itself or maybe an external configuration file.
	ret->measuringPointID = "1";

	return ret;
}


static UA_StatusCode
serverSetHistoryData_sqlite(UA_Server *server,
	void *hdbContext,
	const UA_NodeId *sessionId,
	void *sessionContext,
	const UA_NodeId *nodeId,
	UA_Boolean historizing,
	const UA_DataValue *value)
{
	//We do not add data to the SQLite database
	return UA_STATUSCODE_GOOD;
}


static size_t
getEnd_sqlite(UA_Server *server,
	void *hdbContext,
	const UA_NodeId *sessionId,
	void *sessionContext,
	const UA_NodeId *nodeId)
{
	return END_OF_DATA;
}


//This is a callback for all queries that return a single timestamp as the number of Unix seconds
static int timestamp_callback(void* result, int count, char **data, char **columns)
{
	*(size_t*)result = convertTimestampStringToUnixSeconds(data[0]);

	return 0;
}


static int resultSize_callback(void* result, int count, char **data, char **columns)
{
	*(size_t*)result = strtol(data[0], NULL, 10);

	return 0;
}


static size_t
lastIndex_sqlite(UA_Server *server,
	void *hdbContext,
	const UA_NodeId *sessionId,
	void *sessionContext,
	const UA_NodeId *nodeId)
{
	struct context_sqlite* context = (struct context_sqlite*)hdbContext;

	size_t result;
	char* errorMessage;

	char query[QUERY_BUFFER_SIZE] = "";
	strcat_s(query, QUERY_BUFFER_SIZE, "SELECT Timestamp FROM PeriodicValues WHERE MeasuringPointID=");
	strcat_s(query, QUERY_BUFFER_SIZE, context->measuringPointID);
	strcat_s(query, QUERY_BUFFER_SIZE, " ORDER BY Timestamp DESC LIMIT 1");
	
	int res = sqlite3_exec(context->sqlite, query, timestamp_callback, &result, &errorMessage);

	if (res != SQLITE_OK)
	{
		//Todo: log error message here
		sqlite3_free(errorMessage);
		return END_OF_DATA;
	}

	return result;

}

static size_t
firstIndex_sqlite(UA_Server *server,
	void *hdbContext,
	const UA_NodeId *sessionId,
	void *sessionContext,
	const UA_NodeId *nodeId)
{
	struct context_sqlite* context = (struct context_sqlite*)hdbContext;

	size_t result;
	char* errorMessage;

	char query[QUERY_BUFFER_SIZE] = "";
	strcat_s(query, QUERY_BUFFER_SIZE, "SELECT Timestamp FROM PeriodicValues WHERE MeasuringPointID=");
	strcat_s(query, QUERY_BUFFER_SIZE, context->measuringPointID);
	strcat_s(query, QUERY_BUFFER_SIZE, " ORDER BY Timestamp LIMIT 1");
	
	int res = sqlite3_exec(context->sqlite, query, timestamp_callback, &result, &errorMessage);

	if (res != SQLITE_OK)
	{
		//Todo: log error message here
		sqlite3_free(errorMessage);
		return END_OF_DATA;
	}

	return result;
}


static UA_Boolean
search_sqlite(struct context_sqlite* context,
	size_t unixSeconds, MatchStrategy strategy,
	size_t *index) 
{	
	*index = END_OF_DATA;
	char* errorMessage;

	char query[QUERY_BUFFER_SIZE] = "";
	strcat_s(query, QUERY_BUFFER_SIZE, "SELECT Timestamp FROM PeriodicValues WHERE MeasuringPointID=");
	strcat_s(query, QUERY_BUFFER_SIZE, context->measuringPointID);
	strcat_s(query, QUERY_BUFFER_SIZE, " AND ");

	switch (strategy)
	{
	case MATCH_EQUAL_OR_AFTER:
		strcat_s(query, QUERY_BUFFER_SIZE, "Timestamp>='");
		strcat_s(query, QUERY_BUFFER_SIZE, convertUnixSecondsToTimestampString(unixSeconds));
		strcat_s(query, QUERY_BUFFER_SIZE, "' ORDER BY Timestamp LIMIT 1");
		break;
	case MATCH_AFTER:
		strcat_s(query, QUERY_BUFFER_SIZE, "Timestamp>'");
		strcat_s(query, QUERY_BUFFER_SIZE, convertUnixSecondsToTimestampString(unixSeconds));
		strcat_s(query, QUERY_BUFFER_SIZE, "' ORDER BY Timestamp LIMIT 1");
		break;
	case MATCH_EQUAL_OR_BEFORE:
		strcat_s(query, QUERY_BUFFER_SIZE, "Timestamp<='");
		strcat_s(query, QUERY_BUFFER_SIZE, convertUnixSecondsToTimestampString(unixSeconds));
		strcat_s(query, QUERY_BUFFER_SIZE, "' ORDER BY Timestamp DESC LIMIT 1");
		break;
	case MATCH_BEFORE:
		strcat_s(query, QUERY_BUFFER_SIZE, "Timestamp<'");
		strcat_s(query, QUERY_BUFFER_SIZE, convertUnixSecondsToTimestampString(unixSeconds));
		strcat_s(query, QUERY_BUFFER_SIZE, "' ORDER BY Timestamp DESC LIMIT 1");
		break;
	default:
		return false;
	}

	
	int res = sqlite3_exec(context->sqlite, query, timestamp_callback, index, &errorMessage);

	if (res != SQLITE_OK)
	{
		//Todo: log error message here
		sqlite3_free(errorMessage);
		return false;
	}
	else
	{
		return true;
	}

}

static size_t
getDateTimeMatch_sqlite(UA_Server *server,
	void *hdbContext,
	const UA_NodeId *sessionId,
	void *sessionContext,
	const UA_NodeId *nodeId,
	const UA_DateTime timestamp,
	const MatchStrategy strategy)
{
	struct context_sqlite* context = (struct context_sqlite*)hdbContext;

	size_t ts = OpcTimestampToUnixSeconds(timestamp);

	size_t result = END_OF_DATA;

	UA_Boolean res = search_sqlite(context, ts, strategy, &result);

	return result;
}


static size_t
resultSize_sqlite(UA_Server *server,
	void *hdbContext,
	const UA_NodeId *sessionId,
	void *sessionContext,
	const UA_NodeId *nodeId,
	size_t startIndex,
	size_t endIndex)
{
	struct context_sqlite* context = (struct context_sqlite*)hdbContext;
	
	char* errorMessage;
	size_t result = 0;

	char query[QUERY_BUFFER_SIZE] = "";
	strcat_s(query, QUERY_BUFFER_SIZE, "SELECT COUNT(*) FROM PeriodicValues WHERE ");
	strcat_s(query, QUERY_BUFFER_SIZE, "(Timestamp>='");
	strcat_s(query, QUERY_BUFFER_SIZE, convertUnixSecondsToTimestampString(startIndex));
	strcat_s(query, QUERY_BUFFER_SIZE, "') AND (Timestamp<='");
	strcat_s(query, QUERY_BUFFER_SIZE, convertUnixSecondsToTimestampString(endIndex));
	strcat_s(query, QUERY_BUFFER_SIZE, "') AND (MeasuringPointID=");
	strcat_s(query, QUERY_BUFFER_SIZE, context->measuringPointID);
	strcat_s(query, QUERY_BUFFER_SIZE, ")");

	int res = sqlite3_exec(context->sqlite, query, resultSize_callback, &result, &errorMessage);

	if (res != SQLITE_OK)
	{
		//Todo: log error message here
		sqlite3_free(errorMessage);		
	}

	return result;
}


static int copyDataValues_callback(void* result, int count, char **data, char **columns)
{	
	UA_DataValue dv;
	UA_DataValue_init(&dv);

	dv.status = UA_STATUSCODE_GOOD;
	dv.hasStatus = true;

	dv.sourceTimestamp = UnixSecondsToOpcTimestamp(convertTimestampStringToUnixSeconds(data[0]));
	dv.hasSourceTimestamp = true;

	dv.serverTimestamp = dv.sourceTimestamp;
	dv.hasServerTimestamp = true;

	double value = strtod(data[1], NULL);

	UA_Variant_setScalarCopy(&dv.value, &value, &UA_TYPES[UA_TYPES_DOUBLE]);
	dv.hasValue = true;

	context_copyDataValues* ctx = (context_copyDataValues*)result;

	UA_DataValue_copy(&dv, &ctx->values[ctx->counter]);

	ctx->counter++;

	if (ctx->counter == ctx->maxValues)
		return 1;
	else
		return 0;
}


static UA_StatusCode
copyDataValues_sqlite(UA_Server *server,
	void *hdbContext,
	const UA_NodeId *sessionId,
	void *sessionContext,
	const UA_NodeId *nodeId,
	size_t startIndex,
	size_t endIndex,
	UA_Boolean reverse,
	size_t maxValues,
	UA_NumericRange range,
	UA_Boolean releaseContinuationPoints,
	const UA_ByteString *continuationPoint,
	UA_ByteString *outContinuationPoint,
	size_t *providedValues,
	UA_DataValue *values)
{
	//NOTE: this demo does not support continuation points!!!

	struct context_sqlite* context = (struct context_sqlite*)hdbContext;
	
	char* errorMessage;
	const char* measuringPointID = "1";

	char query[QUERY_BUFFER_SIZE] = "";
	strcat_s(query, QUERY_BUFFER_SIZE, "SELECT Timestamp, Value FROM PeriodicValues WHERE ");
	strcat_s(query, QUERY_BUFFER_SIZE, "(Timestamp>='");
	strcat_s(query, QUERY_BUFFER_SIZE, convertUnixSecondsToTimestampString(startIndex));
	strcat_s(query, QUERY_BUFFER_SIZE, "') AND (Timestamp<='");
	strcat_s(query, QUERY_BUFFER_SIZE, convertUnixSecondsToTimestampString(endIndex));
	strcat_s(query, QUERY_BUFFER_SIZE, "') AND (MeasuringPointID=");
	strcat_s(query, QUERY_BUFFER_SIZE, measuringPointID);
	strcat_s(query, QUERY_BUFFER_SIZE, ")");
	
	context_copyDataValues ctx;
	ctx.maxValues = maxValues;
	ctx.counter = 0;
	ctx.values = values;

	int res = sqlite3_exec(context->sqlite, query, copyDataValues_callback, &ctx, &errorMessage);

	if (res != SQLITE_OK)
	{
		//Todo: log error message here
		sqlite3_free(errorMessage);

		if (res == SQLITE_ABORT)
			return UA_STATUSCODE_GOOD;
		else
			return UA_STATUSCODE_BADINTERNALERROR;
	}
	else
	{
		return UA_STATUSCODE_GOOD;
	}
	
}

static const UA_DataValue*
getDataValue_sqlite(UA_Server *server,
	void *hdbContext,
	const UA_NodeId *sessionId,
	void *sessionContext,
	const UA_NodeId *nodeId,
	size_t index)
{
	struct context_sqlite* context = (struct context_sqlite*)hdbContext;
	

	return NULL;
}


static UA_Boolean
boundSupported_sqlite(UA_Server *server,
	void *hdbContext,
	const UA_NodeId *sessionId,
	void *sessionContext,
	const UA_NodeId *nodeId)
{
	return false; // We don't support returning bounds in this demo
}


static UA_Boolean
timestampsToReturnSupported_sqlite(UA_Server *server,
	void *hdbContext,
	const UA_NodeId *sessionId,
	void *sessionContext,
	const UA_NodeId *nodeId,
	const UA_TimestampsToReturn timestampsToReturn)
{
	return true;
}


UA_HistoryDataBackend
UA_HistoryDataBackend_sqlite(const char* filename)
{
	UA_HistoryDataBackend result;
	memset(&result, 0, sizeof(UA_HistoryDataBackend));
	result.serverSetHistoryData = &serverSetHistoryData_sqlite;
	result.resultSize = &resultSize_sqlite;
	result.getEnd = &getEnd_sqlite;
	result.lastIndex = &lastIndex_sqlite;
	result.firstIndex = &firstIndex_sqlite;
	result.getDateTimeMatch = &getDateTimeMatch_sqlite;
	result.copyDataValues = &copyDataValues_sqlite;
	result.getDataValue = &getDataValue_sqlite;
	result.boundSupported = &boundSupported_sqlite;
	result.timestampsToReturnSupported = &timestampsToReturnSupported_sqlite;
	result.deleteMembers = NULL;// We don't support deleting in this demo
	result.getHistoryData = NULL;// We don't support the high level API in this demo
	result.context = generateContext_sqlite(filename);
	return result;
}


#endif