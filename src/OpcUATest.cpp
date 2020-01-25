#include <signal.h>
#include <exception>
#include <iostream>
#include "open62541.h"
#include "SQLiteBackend.h"


#pragma comment(lib, "ws2_32.lib")


size_t getDateTimeMatch_backend_flashFiler(UA_Server *server,
	void *hdbContext,
	const UA_NodeId *sessionId,
	void *sessionContext,
	const UA_NodeId *nodeId,
	const UA_DateTime timestamp,
	const MatchStrategy strategy)
{
	return 100000;
}


size_t getEnd_backend_flashFiler(UA_Server *server,
	void *hdbContext,
	const UA_NodeId *sessionId,
	void *sessionContext,
	const UA_NodeId *nodeId)
{
	return 100000;
}


size_t resultSize_backend_flashFiler(UA_Server *server,
	void *hdbContext,
	const UA_NodeId *sessionId,
	void *sessionContext,
	const UA_NodeId *nodeId,
	size_t startIndex,
	size_t endIndex)
{
	return 2;
}


UA_Boolean timestampsToReturnSupported_backend_flashFiler(UA_Server *server,
	void *hdbContext,
	const UA_NodeId *sessionId,
	void *sessionContext,
	const UA_NodeId *nodeId,
	const UA_TimestampsToReturn timestampsToReturn)
{
	return UA_TRUE;
}

UA_HistoryDataBackend
UA_HistoryDataBackend_FlashFiler() {
	
	UA_HistoryDataBackend result;
	memset(&result, 0, sizeof(UA_HistoryDataBackend));

	/*UA_MemoryStoreContext *ctx = (UA_MemoryStoreContext *)UA_calloc(1, sizeof(UA_MemoryStoreContext));
	if (!ctx)
		return result;

	ctx->dataStore = (UA_NodeIdStoreContextItem_backend_memory*)UA_calloc(initialNodeIdStoreSize, sizeof(UA_NodeIdStoreContextItem_backend_memory));
	ctx->initialStoreSize = initialDataStoreSize;
	ctx->storeSize = initialNodeIdStoreSize;
	ctx->storeEnd = 0;*/

	//result.serverSetHistoryData = &serverSetHistoryData_backend_memory;
	result.resultSize = &resultSize_backend_flashFiler;
	result.getEnd = &getEnd_backend_flashFiler;
	//result.lastIndex = &lastIndex_backend_memory;
	//result.firstIndex = &firstIndex_backend_memory;
	result.getDateTimeMatch = &getDateTimeMatch_backend_flashFiler;
	//result.copyDataValues = &copyDataValues_backend_memory;
	//result.getDataValue = &getDataValue_backend_memory;
	//result.boundSupported = &boundSupported_backend_memory;*/
	result.timestampsToReturnSupported = &timestampsToReturnSupported_backend_flashFiler;
	/*result.deleteMembers = deleteMembers_backend_memory;
	result.getHistoryData = NULL;*/
	//result.context = ctx;
	return result;
}



static UA_Boolean running = true;
static void stopHandler(int sign) {
	(void)sign;
	UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "received ctrl-c");
	running = false;
}


int main(void) {
	signal(SIGINT, stopHandler);
	signal(SIGTERM, stopHandler);


	UA_ServerConfig *config = UA_ServerConfig_new_default();
	
	/*
	 * We need a gathering for the plugin to constuct.
	 * The UA_HistoryDataGathering is responsible to collect data and store it to the database.
	 * We will use this gathering for one node, only. initialNodeIdStoreSize = 1
	 * The store will grow if you register more than one node, but this is expensive.
	 */
	UA_HistoryDataGathering gathering = UA_HistoryDataGathering_Default(1);
	
	/*
	 * We set the responsible plugin in the configuration.
	 * UA_HistoryDatabase is the main plugin which handles the historical data service.
	 */
	config->historyDatabase = UA_HistoryDatabase_default(gathering);

	UA_Server *server = UA_Server_new(config);
	
	/* Define the attribute of the uint32 variable node */
	UA_VariableAttributes attr = UA_VariableAttributes_default;
	UA_Double myDouble = 17.2;
	UA_Variant_setScalar(&attr.value, &myDouble, &UA_TYPES[UA_TYPES_DOUBLE]);
	attr.description = UA_LOCALIZEDTEXT("en-US", "myDoubleValue");
	attr.displayName = UA_LOCALIZEDTEXT("en-US", "myDoubleValue");
	attr.dataType = UA_TYPES[UA_TYPES_DOUBLE].typeId;
	
	/*
	 * We set the access level to also support history read
	 * This is what will be reported to clients
	 */
	attr.accessLevel = UA_ACCESSLEVELMASK_READ | UA_ACCESSLEVELMASK_HISTORYREAD;
	
	/*
	 * We also set this node to historizing, so the server internals also know from it.
	 */
	attr.historizing = true;

	/* Add the variable node to the information model */
	UA_NodeId doubleNodeId = UA_NODEID_STRING(1, "myDoubleValue");
	UA_QualifiedName doubleName = UA_QUALIFIEDNAME(1, "myDoubleValue");
	UA_NodeId parentNodeId = UA_NODEID_NUMERIC(0, UA_NS0ID_OBJECTSFOLDER);
	UA_NodeId parentReferenceNodeId = UA_NODEID_NUMERIC(0, UA_NS0ID_ORGANIZES);
	UA_NodeId outNodeId;
	UA_NodeId_init(&outNodeId);
	UA_StatusCode retval = UA_Server_addVariableNode(server,
		doubleNodeId,
		parentNodeId,
		parentReferenceNodeId,
		doubleName,
		UA_NODEID_NUMERIC(0, UA_NS0ID_BASEDATAVARIABLETYPE),
		attr,
		NULL,
		&outNodeId);

	UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "UA_Server_addVariableNode %s", UA_StatusCode_name(retval));
	
	/*
	 * Now we define the settings for our node
	 */
	UA_HistorizingNodeIdSettings setting;
	
	
	setting.historizingBackend = UA_HistoryDataBackend_sqlite("database.sqlite");
	
	/*
	 * We want the server to serve a maximum of 100 values per request.
	 * This value depend on the plattform you are running the server.
	 * A big server can serve more values, smaller ones less.
	 */
	setting.maxHistoryDataResponseSize = 100;


	setting.historizingUpdateStrategy = UA_HISTORIZINGUPDATESTRATEGY_VALUESET;
	
	/*
	 * At the end we register the node for gathering data in the database.
	 */
	retval = gathering.registerNodeId(server, gathering.context, &outNodeId, setting);
	UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "registerNodeId %s", UA_StatusCode_name(retval));
	
	
	retval = UA_Server_run(server, &running);
	UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "UA_Server_run %s", UA_StatusCode_name(retval));
	
	UA_Server_run_shutdown(server);
	UA_Server_delete(server);
	UA_ServerConfig_delete(config);

	return (int)retval;
}