/*
 * Copyright 2023 Conductor Authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.es8.dao.index;

import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NByteArrayEntity;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.support.RetryTemplate;

import com.netflix.conductor.annotations.Trace;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.tasks.TaskExecLog;
import com.netflix.conductor.common.run.SearchResult;
import com.netflix.conductor.common.run.TaskSummary;
import com.netflix.conductor.common.run.WorkflowSummary;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.exception.NonTransientException;
import com.netflix.conductor.core.exception.TransientException;
import com.netflix.conductor.dao.IndexDAO;
import com.netflix.conductor.es8.config.ElasticSearchProperties;
import com.netflix.conductor.es8.dao.query.parser.internal.ParserException;
import com.netflix.conductor.metrics.Monitors;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch._types.query_dsl.*;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.json.JsonData;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

@Trace
public class ElasticSearchRestDAOV8 extends ElasticSearchBaseDAO implements IndexDAO {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchRestDAOV8.class);

    private static final String CLASS_NAME = ElasticSearchRestDAOV8.class.getSimpleName();

    private static final int CORE_POOL_SIZE = 6;
    private static final long KEEP_ALIVE_TIME = 1L;

    private static final String WORKFLOW_DOC_TYPE = "workflow";
    private static final String TASK_DOC_TYPE = "task";
    private static final String LOG_DOC_TYPE = "task_log";
    private static final String EVENT_DOC_TYPE = "event";
    private static final String MSG_DOC_TYPE = "message";

    private static final TimeZone GMT = TimeZone.getTimeZone("GMT");
    private static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("yyyyMMWW");

    private @interface HttpMethod {

        String GET = "GET";
        String POST = "POST";
        String PUT = "PUT";
        String HEAD = "HEAD";
    }

    private static final String className = ElasticSearchRestDAOV8.class.getSimpleName();

    private final String workflowIndexName;
    private final String taskIndexName;
    private final String eventIndexPrefix;
    private String eventIndexName;
    private final String messageIndexPrefix;
    private String messageIndexName;
    private String logIndexName;
    private final String logIndexPrefix;

    private final String clusterHealthColor;
    private final RestClient elasticSearchAdminClient;
    private final ExecutorService executorService;
    private final ExecutorService logExecutorService;
    private final ConcurrentHashMap<String, BulkRequests> bulkRequests;
    private final int indexBatchSize;
    private final int asyncBufferFlushTimeout;
    private final ElasticSearchProperties properties;
    private final RetryTemplate retryTemplate;
    private final ElasticsearchClient elasticsearchClient;

    static {
        SIMPLE_DATE_FORMAT.setTimeZone(GMT);
    }

    public ElasticSearchRestDAOV8(
            RestClient restClient,
            RetryTemplate retryTemplate,
            ElasticSearchProperties properties,
            ObjectMapper objectMapper,
            ElasticsearchClient elasticsearchClient) {

        this.objectMapper = objectMapper;
        this.elasticSearchAdminClient = restClient;
        this.elasticsearchClient = elasticsearchClient;
        this.clusterHealthColor = properties.getClusterHealthColor();
        this.bulkRequests = new ConcurrentHashMap<>();
        this.indexBatchSize = properties.getIndexBatchSize();
        this.asyncBufferFlushTimeout = (int) properties.getAsyncBufferFlushTimeout().getSeconds();
        this.properties = properties;

        this.indexPrefix = properties.getIndexPrefix();

        this.workflowIndexName = getIndexName(WORKFLOW_DOC_TYPE);
        this.taskIndexName = getIndexName(TASK_DOC_TYPE);
        this.logIndexPrefix = this.indexPrefix + "_" + LOG_DOC_TYPE;
        this.messageIndexPrefix = this.indexPrefix + "_" + MSG_DOC_TYPE;
        this.eventIndexPrefix = this.indexPrefix + "_" + EVENT_DOC_TYPE;
        int workerQueueSize = properties.getAsyncWorkerQueueSize();
        int maximumPoolSize = properties.getAsyncMaxPoolSize();

        // Set up a workerpool for performing async operations.
        this.executorService =
                new ThreadPoolExecutor(
                        CORE_POOL_SIZE,
                        maximumPoolSize,
                        KEEP_ALIVE_TIME,
                        TimeUnit.MINUTES,
                        new LinkedBlockingQueue<>(workerQueueSize),
                        (runnable, executor) -> {
                            logger.warn(
                                    "Request  {} to async dao discarded in executor {}",
                                    runnable,
                                    executor);
                            Monitors.recordDiscardedIndexingCount("indexQueue");
                        });

        // Set up a workerpool for performing async operations for task_logs,
        // event_executions,
        // message
        int corePoolSize = 1;
        maximumPoolSize = 2;
        long keepAliveTime = 30L;
        this.logExecutorService =
                new ThreadPoolExecutor(
                        corePoolSize,
                        maximumPoolSize,
                        keepAliveTime,
                        TimeUnit.SECONDS,
                        new LinkedBlockingQueue<>(workerQueueSize),
                        (runnable, executor) -> {
                            logger.warn(
                                    "Request {} to async log dao discarded in executor {}",
                                    runnable,
                                    executor);
                            Monitors.recordDiscardedIndexingCount("logQueue");
                        });

        Executors.newSingleThreadScheduledExecutor()
                .scheduleAtFixedRate(this::flushBulkRequests, 60, 30, TimeUnit.SECONDS);
        this.retryTemplate = retryTemplate;
    }

    @PreDestroy
    private void shutdown() {
        logger.info("Gracefully shutdown executor service");
        shutdownExecutorService(logExecutorService);
        shutdownExecutorService(executorService);
    }

    private void shutdownExecutorService(ExecutorService execService) {
        try {
            execService.shutdown();
            if (execService.awaitTermination(30, TimeUnit.SECONDS)) {
                logger.debug("tasks completed, shutting down");
            } else {
                logger.warn("Forcing shutdown after waiting for 30 seconds");
                execService.shutdownNow();
            }
        } catch (InterruptedException ie) {
            logger.warn(
                    "Shutdown interrupted, invoking shutdownNow on scheduledThreadPoolExecutor for delay queue");
            execService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    @Override
    @PostConstruct
    public void setup() throws Exception {
        waitForHealthyCluster();

        if (properties.isAutoIndexManagementEnabled()) {
            createIndexesTemplates();
            createWorkflowIndex();
            createTaskIndex();
        }
    }

    private void createIndexesTemplates() {
        try {
            initIndexesTemplates();
            updateIndexesNames();
            Executors.newSingleThreadScheduledExecutor()
                    .scheduleAtFixedRate(this::updateIndexesNames, 0, 1, TimeUnit.HOURS);
        } catch (Exception e) {
            logger.error("Error creating index templates!", e);
        }
    }

    private void initIndexesTemplates() {
        initIndexTemplate(LOG_DOC_TYPE);
        initIndexTemplate(EVENT_DOC_TYPE);
        initIndexTemplate(MSG_DOC_TYPE);
    }

    /** Initializes the index with the required templates and mappings. */
    private void initIndexTemplate(String type) {
        String template = "template_" + type;
        try {
            if (doesResourceNotExist("/_template/" + template)) {
                logger.info("Creating the index template '" + template + "'");
                InputStream stream =
                        ElasticSearchRestDAOV8.class.getResourceAsStream("/" + template + ".json");
                byte[] templateSource = IOUtils.toByteArray(stream);

                HttpEntity entity =
                        new NByteArrayEntity(templateSource, ContentType.APPLICATION_JSON);
                Request request = new Request(HttpMethod.PUT, "/_template/" + template);
                request.setEntity(entity);
                elasticSearchAdminClient.performRequest(request);
            }
        } catch (Exception e) {
            logger.error("Failed to init " + template, e);
        }
    }

    private void updateIndexesNames() {
        logIndexName = updateIndexName(LOG_DOC_TYPE);
        eventIndexName = updateIndexName(EVENT_DOC_TYPE);
        messageIndexName = updateIndexName(MSG_DOC_TYPE);
    }

    private String updateIndexName(String type) {
        String indexName =
                this.indexPrefix + "_" + type + "_" + SIMPLE_DATE_FORMAT.format(new Date());
        try {
            addIndex(indexName);
            return indexName;
        } catch (IOException e) {
            logger.error("Failed to update log index name: {}", indexName, e);
            throw new NonTransientException(e.getMessage(), e);
        }
    }

    private void createWorkflowIndex() {
        String indexName = getIndexName(WORKFLOW_DOC_TYPE);
        try {
            addIndex(indexName, "/mappings_docType_workflow.json");
        } catch (IOException e) {
            logger.error("Failed to initialize index '{}'", indexName, e);
        }
    }

    private void createTaskIndex() {
        String indexName = getIndexName(TASK_DOC_TYPE);
        try {
            addIndex(indexName, "/mappings_docType_task.json");
        } catch (IOException e) {
            logger.error("Failed to initialize index '{}'", indexName, e);
        }
    }

    /**
     * Waits for the ES cluster to become green.
     *
     * @throws Exception If there is an issue connecting with the ES cluster.
     */
    private void waitForHealthyCluster() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("wait_for_status", this.clusterHealthColor);
        params.put("timeout", "30s");
        Request request = new Request(HttpMethod.GET, "/_cluster/health");
        request.addParameters(params);
        elasticSearchAdminClient.performRequest(request);
    }

    /**
     * Adds an index to elasticsearch if it does not exist.
     *
     * @param index The name of the index to create.
     * @param mappingFilename Index mapping filename
     * @throws IOException If an error occurred during requests to ES.
     */
    private void addIndex(String index, final String mappingFilename) throws IOException {
        logger.info("Adding index '{}'...", index);
        String resourcePath = "/" + index;
        if (doesResourceNotExist(resourcePath)) {
            try {
                ObjectNode setting = objectMapper.createObjectNode();
                ObjectNode indexSetting = objectMapper.createObjectNode();
                ObjectNode root = objectMapper.createObjectNode();
                indexSetting.put("number_of_shards", properties.getIndexShardCount());
                indexSetting.put("number_of_replicas", properties.getIndexReplicasCount());
                JsonNode mappingNodeValue =
                        objectMapper.readTree(loadTypeMappingSource(mappingFilename));
                root.set("settings", indexSetting);
                root.set("mappings", mappingNodeValue);
                Request request = new Request(HttpMethod.PUT, resourcePath);
                request.setEntity(
                        new NStringEntity(
                                objectMapper.writeValueAsString(root),
                                ContentType.APPLICATION_JSON));
                elasticSearchAdminClient.performRequest(request);
                logger.info("Added '{}' index", index);
            } catch (ResponseException e) {

                boolean errorCreatingIndex = true;

                Response errorResponse = e.getResponse();
                if (errorResponse.getStatusLine().getStatusCode() == HttpStatus.SC_BAD_REQUEST) {
                    JsonNode root =
                            objectMapper.readTree(EntityUtils.toString(errorResponse.getEntity()));
                    String errorCode = root.get("error").get("type").asText();
                    if ("index_already_exists_exception".equals(errorCode)) {
                        errorCreatingIndex = false;
                    }
                }

                if (errorCreatingIndex) {
                    throw e;
                }
            }
        } else {
            logger.info("Index '{}' already exists", index);
        }
    }

    /**
     * Adds an index to elasticsearch if it does not exist.
     *
     * @param index The name of the index to create.
     * @throws IOException If an error occurred during requests to ES.
     */
    private void addIndex(final String index) throws IOException {

        logger.info("Adding index '{}'...", index);

        String resourcePath = "/" + index;

        if (doesResourceNotExist(resourcePath)) {

            try {
                ObjectNode setting = objectMapper.createObjectNode();
                ObjectNode indexSetting = objectMapper.createObjectNode();

                indexSetting.put("number_of_shards", properties.getIndexShardCount());
                indexSetting.put("number_of_replicas", properties.getIndexReplicasCount());

                setting.set("settings", indexSetting);

                Request request = new Request(HttpMethod.PUT, resourcePath);
                request.setEntity(
                        new NStringEntity(setting.toString(), ContentType.APPLICATION_JSON));
                elasticSearchAdminClient.performRequest(request);
                logger.info("Added '{}' index", index);
            } catch (ResponseException e) {

                boolean errorCreatingIndex = true;

                Response errorResponse = e.getResponse();
                if (errorResponse.getStatusLine().getStatusCode() == HttpStatus.SC_BAD_REQUEST) {
                    JsonNode root =
                            objectMapper.readTree(EntityUtils.toString(errorResponse.getEntity()));
                    String errorCode = root.get("error").get("type").asText();
                    if ("resource_already_exists_exception".equals(errorCode)) {
                        errorCreatingIndex = false;
                    }
                }

                if (errorCreatingIndex) {
                    throw e;
                }
            }
        } else {
            logger.info("Index '{}' already exists", index);
        }
    }

    /**
     * Determines whether a resource exists in ES. This will call a GET method to a particular path
     * and return true if status 200; false otherwise.
     *
     * @param resourcePath The path of the resource to get.
     * @return True if it exists; false otherwise.
     * @throws IOException If an error occurred during requests to ES.
     */
    public boolean doesResourceExist(final String resourcePath) throws IOException {
        Request request = new Request(HttpMethod.HEAD, resourcePath);
        Response response = elasticSearchAdminClient.performRequest(request);
        return response.getStatusLine().getStatusCode() == HttpStatus.SC_OK;
    }

    /**
     * The inverse of doesResourceExist.
     *
     * @param resourcePath The path of the resource to check.
     * @return True if it does not exist; false otherwise.
     * @throws IOException If an error occurred during requests to ES.
     */
    public boolean doesResourceNotExist(final String resourcePath) throws IOException {
        return !doesResourceExist(resourcePath);
    }

    @Override
    public void indexWorkflow(WorkflowSummary workflow) {
        try {
            long startTime = Instant.now().toEpochMilli();
            String workflowId = workflow.getWorkflowId();

            IndexRequest<WorkflowSummary> request =
                    IndexRequest.of(
                            i -> i.index(workflowIndexName).id(workflowId).document(workflow));

            elasticsearchClient.index(request);
            long endTime = Instant.now().toEpochMilli();
            logger.debug(
                    "Time taken {} for indexing workflow: {}", endTime - startTime, workflowId);
            Monitors.recordESIndexTime("index_workflow", WORKFLOW_DOC_TYPE, endTime - startTime);
            Monitors.recordWorkerQueueSize(
                    "indexQueue", ((ThreadPoolExecutor) executorService).getQueue().size());
        } catch (Exception e) {
            Monitors.error(className, "indexWorkflow");
            logger.error("Failed to index workflow: {}", workflow.getWorkflowId(), e);
        }
    }

    @Override
    public CompletableFuture<Void> asyncIndexWorkflow(WorkflowSummary workflow) {
        return CompletableFuture.runAsync(() -> indexWorkflow(workflow), executorService);
    }

    @Override
    public void indexTask(TaskSummary task) {
        try {
            long startTime = Instant.now().toEpochMilli();
            String taskId = task.getTaskId();

            indexObject(taskIndexName, TASK_DOC_TYPE, taskId, task);
            long endTime = Instant.now().toEpochMilli();
            logger.debug(
                    "Time taken {} for  indexing task:{} in workflow: {}",
                    endTime - startTime,
                    taskId,
                    task.getWorkflowId());
            Monitors.recordESIndexTime("index_task", TASK_DOC_TYPE, endTime - startTime);
            Monitors.recordWorkerQueueSize(
                    "indexQueue", ((ThreadPoolExecutor) executorService).getQueue().size());
        } catch (Exception e) {
            logger.error("Failed to index task: {}", task.getTaskId(), e);
        }
    }

    @Override
    public CompletableFuture<Void> asyncIndexTask(TaskSummary task) {
        return CompletableFuture.runAsync(() -> indexTask(task), executorService);
    }

    @Override
    public void addTaskExecutionLogs(List<TaskExecLog> taskExecLogs) {
        if (taskExecLogs.isEmpty()) {
            return;
        }

        try {
            long startTime = Instant.now().toEpochMilli();
            List<BulkOperation> bulkOperations = new ArrayList<>();
            for (TaskExecLog log : taskExecLogs) {
                bulkOperations.add(
                        BulkOperation.of(
                                bo -> bo.index(ib -> ib.index(logIndexName).document(log))));
            }

            elasticsearchClient.bulk(b -> b.operations(bulkOperations));
            long endTime = Instant.now().toEpochMilli();
            logger.debug("Time taken {} for indexing taskExecutionLogs", endTime - startTime);
            Monitors.recordESIndexTime(
                    "index_task_execution_logs", LOG_DOC_TYPE, endTime - startTime);
            Monitors.recordWorkerQueueSize(
                    "logQueue", ((ThreadPoolExecutor) logExecutorService).getQueue().size());
        } catch (Exception e) {
            List<String> taskIds =
                    taskExecLogs.stream().map(TaskExecLog::getTaskId).collect(Collectors.toList());
            logger.error("Failed to index task execution logs for tasks: {}", taskIds, e);
        }
    }

    @Override
    public CompletableFuture<Void> asyncAddTaskExecutionLogs(List<TaskExecLog> logs) {
        return CompletableFuture.runAsync(() -> addTaskExecutionLogs(logs), logExecutorService);
    }

    @Override
    public List<TaskExecLog> getTaskExecutionLogs(String taskId) {
        try {
            var query = boolQueryBuilder("taskId='" + taskId + "'", "*");

            SearchRequest searchRequest =
                    SearchRequest.of(
                            s ->
                                    s.index(logIndexPrefix + "*")
                                            .query(query)
                                            .sort(o -> o.field(f -> f.field("createdTime")))
                                            .size(properties.getTaskLogResultLimit()));

            SearchResponse<TaskExecLog> response =
                    elasticsearchClient.search(searchRequest, TaskExecLog.class);

            return response.hits().hits().stream()
                    .map(h -> h.source())
                    .collect(Collectors.toList());
        } catch (Exception e) {
            logger.error("Failed to get task execution logs for task: {}", taskId, e);
        }
        return null;
    }

    @Override
    public List<Message> getMessages(String queue) {
        try {
            var query = boolQueryBuilder("queue='" + queue + "'", "*");

            SearchRequest searchRequest =
                    SearchRequest.of(
                            s ->
                                    s.index(messageIndexPrefix + "*")
                                            .query(query)
                                            .sort(o -> o.field(f -> f.field("created"))));

            SearchResponse<Map> response = elasticsearchClient.search(searchRequest, Map.class);

            TypeFactory factory = TypeFactory.defaultInstance();
            MapType type = factory.constructMapType(HashMap.class, String.class, String.class);
            List<Message> messages = new ArrayList<>(response.hits().hits().size());
            for (var hit : response.hits().hits()) {
                Map<String, String> mapSource = (Map<String, String>) hit.source();
                Message msg =
                        new Message(mapSource.get("messageId"), mapSource.get("payload"), null);
                messages.add(msg);
            }
            return messages;
        } catch (Exception e) {
            logger.error("Failed to get messages for queue: {}", queue, e);
        }
        return null;
    }

    @Override
    public List<EventExecution> getEventExecutions(String event) {
        try {
            var query = boolQueryBuilder("event='" + event + "'", "*");

            SearchRequest searchRequest =
                    SearchRequest.of(
                            s ->
                                    s.index(eventIndexPrefix + "*")
                                            .query(query)
                                            .sort(o -> o.field(f -> f.field("created"))));

            SearchResponse<EventExecution> response =
                    elasticsearchClient.search(searchRequest, EventExecution.class);

            return response.hits().hits().stream()
                    .map(h -> h.source())
                    .collect(Collectors.toList());
        } catch (Exception e) {
            logger.error("Failed to get executions for event: {}", event, e);
        }
        return null;
    }

    @Override
    public void addMessage(String queue, Message message) {
        try {
            long startTime = Instant.now().toEpochMilli();
            Map<String, Object> doc = new HashMap<>();
            doc.put("messageId", message.getId());
            doc.put("payload", message.getPayload());
            doc.put("queue", queue);
            doc.put("created", System.currentTimeMillis());

            indexObject(messageIndexName, MSG_DOC_TYPE, doc);
            long endTime = Instant.now().toEpochMilli();
            logger.debug(
                    "Time taken {} for  indexing message: {}",
                    endTime - startTime,
                    message.getId());
            Monitors.recordESIndexTime("add_message", MSG_DOC_TYPE, endTime - startTime);
        } catch (Exception e) {
            logger.error("Failed to index message: {}", message.getId(), e);
        }
    }

    @Override
    public CompletableFuture<Void> asyncAddMessage(String queue, Message message) {
        return CompletableFuture.runAsync(() -> addMessage(queue, message), executorService);
    }

    @Override
    public void addEventExecution(EventExecution eventExecution) {
        try {
            long startTime = Instant.now().toEpochMilli();
            String id =
                    eventExecution.getName()
                            + "."
                            + eventExecution.getEvent()
                            + "."
                            + eventExecution.getMessageId()
                            + "."
                            + eventExecution.getId();

            indexObject(eventIndexName, EVENT_DOC_TYPE, id, eventExecution);
            long endTime = Instant.now().toEpochMilli();
            logger.debug(
                    "Time taken {} for indexing event execution: {}",
                    endTime - startTime,
                    eventExecution.getId());
            Monitors.recordESIndexTime("add_event_execution", EVENT_DOC_TYPE, endTime - startTime);
            Monitors.recordWorkerQueueSize(
                    "logQueue", ((ThreadPoolExecutor) logExecutorService).getQueue().size());
        } catch (Exception e) {
            logger.error("Failed to index event execution: {}", eventExecution.getId(), e);
        }
    }

    @Override
    public CompletableFuture<Void> asyncAddEventExecution(EventExecution eventExecution) {
        return CompletableFuture.runAsync(
                () -> addEventExecution(eventExecution), logExecutorService);
    }

    @Override
    public SearchResult<String> searchWorkflows(
            String query, String freeText, int start, int count, List<String> sort) {
        try {
            return searchObjects(
                    getIndexName(WORKFLOW_DOC_TYPE),
                    boolQueryBuilder(query, freeText),
                    start,
                    count,
                    sort,
                    true,
                    String.class);
        } catch (Exception e) {
            throw new NonTransientException(e.getMessage(), e);
        }
    }

    @Override
    public SearchResult<WorkflowSummary> searchWorkflowSummary(
            String query, String freeText, int start, int count, List<String> sort) {
        try {
            return searchObjects(
                    getIndexName(WORKFLOW_DOC_TYPE),
                    boolQueryBuilder(query, freeText),
                    start,
                    count,
                    sort,
                    false,
                    WorkflowSummary.class);
        } catch (Exception e) {
            throw new TransientException(e.getMessage(), e);
        }
    }

    @Override
    public SearchResult<String> searchTasks(
            String query, String freeText, int start, int count, List<String> sort) {
        try {
            return searchObjects(
                    getIndexName(TASK_DOC_TYPE),
                    boolQueryBuilder(query, freeText),
                    start,
                    count,
                    sort,
                    true,
                    String.class);
        } catch (Exception e) {
            throw new NonTransientException(e.getMessage(), e);
        }
    }

    @Override
    public SearchResult<TaskSummary> searchTaskSummary(
            String query, String freeText, int start, int count, List<String> sort) {
        try {
            return searchObjects(
                    getIndexName(TASK_DOC_TYPE),
                    boolQueryBuilder(query, freeText),
                    start,
                    count,
                    sort,
                    false,
                    TaskSummary.class);
        } catch (Exception e) {
            throw new TransientException(e.getMessage(), e);
        }
    }

    @Override
    public void removeWorkflow(String workflowId) {
        try {
            long startTime = Instant.now().toEpochMilli();
            DeleteRequest request =
                    DeleteRequest.of(d -> d.index(workflowIndexName).id(workflowId));

            DeleteResponse response = elasticsearchClient.delete(request);

            if (response.result() == Result.NotFound) {
                logger.error("Index removal failed - document not found by id: {}", workflowId);
            }
            long endTime = Instant.now().toEpochMilli();
            logger.debug(
                    "Time taken {} for removing workflow: {}", endTime - startTime, workflowId);
            Monitors.recordESIndexTime("remove_workflow", WORKFLOW_DOC_TYPE, endTime - startTime);
            Monitors.recordWorkerQueueSize(
                    "indexQueue", ((ThreadPoolExecutor) executorService).getQueue().size());
        } catch (IOException e) {
            logger.error("Failed to remove workflow {} from index", workflowId, e);
            Monitors.error(className, "remove");
        }
    }

    @Override
    public CompletableFuture<Void> asyncRemoveWorkflow(String workflowId) {
        return CompletableFuture.runAsync(() -> removeWorkflow(workflowId), executorService);
    }

    @Override
    public void updateWorkflow(String workflowInstanceId, String[] keys, Object[] values) {
        try {
            if (keys.length != values.length) {
                throw new NonTransientException("Number of keys and values do not match");
            }

            long startTime = Instant.now().toEpochMilli();
            Map<String, Object> source =
                    IntStream.range(0, keys.length)
                            .boxed()
                            .collect(Collectors.toMap(i -> keys[i], i -> values[i]));

            UpdateRequest<Map, Map> request =
                    UpdateRequest.of(
                            u ->
                                    u.index(workflowIndexName)
                                            .id(workflowInstanceId)
                                            .doc(source)
                                            .docAsUpsert(true));

            logger.debug("Updating workflow {} with {}", workflowInstanceId, source);
            elasticsearchClient.update(request, Map.class);
            long endTime = Instant.now().toEpochMilli();
            logger.debug(
                    "Time taken {} for updating workflow: {}",
                    endTime - startTime,
                    workflowInstanceId);
            Monitors.recordESIndexTime("update_workflow", WORKFLOW_DOC_TYPE, endTime - startTime);
            Monitors.recordWorkerQueueSize(
                    "indexQueue", ((ThreadPoolExecutor) executorService).getQueue().size());
        } catch (Exception e) {
            logger.error("Failed to update workflow {}", workflowInstanceId, e);
            Monitors.error(className, "update");
        }
    }

    @Override
    public void removeTask(String workflowId, String taskId) {
        long startTime = Instant.now().toEpochMilli();

        SearchResult<String> taskSearchResult =
                searchTasks(
                        String.format("(taskId='%s') AND (workflowId='%s')", taskId, workflowId),
                        "*",
                        0,
                        1,
                        null);

        if (taskSearchResult.getTotalHits() == 0) {
            logger.error("Task: {} does not belong to workflow: {}", taskId, workflowId);
            Monitors.error(className, "removeTask");
            return;
        }

        DeleteRequest request = DeleteRequest.of(d -> d.index(taskIndexName).id(taskId));

        try {
            DeleteResponse response = elasticsearchClient.delete(request);

            if (response.result() != Result.Deleted) {
                logger.error("Index removal failed - task not found by id: {}", workflowId);
                Monitors.error(className, "removeTask");
                return;
            }
            long endTime = Instant.now().toEpochMilli();
            logger.debug(
                    "Time taken {} for removing task:{} of workflow: {}",
                    endTime - startTime,
                    taskId,
                    workflowId);
            Monitors.recordESIndexTime("remove_task", "", endTime - startTime);
            Monitors.recordWorkerQueueSize(
                    "indexQueue", ((ThreadPoolExecutor) executorService).getQueue().size());
        } catch (IOException e) {
            logger.error(
                    "Failed to remove task {} of workflow: {} from index", taskId, workflowId, e);
            Monitors.error(className, "removeTask");
        }
    }

    @Override
    public CompletableFuture<Void> asyncRemoveTask(String workflowId, String taskId) {
        return CompletableFuture.runAsync(() -> removeTask(workflowId, taskId), executorService);
    }

    @Override
    public void updateTask(String workflowId, String taskId, String[] keys, Object[] values) {
        try {
            if (keys.length != values.length) {
                throw new IllegalArgumentException("Number of keys and values do not match");
            }

            long startTime = Instant.now().toEpochMilli();
            Map<String, Object> source =
                    IntStream.range(0, keys.length)
                            .boxed()
                            .collect(Collectors.toMap(i -> keys[i], i -> values[i]));

            UpdateRequest<Map, Map> request =
                    UpdateRequest.of(
                            u -> u.index(taskIndexName).id(taskId).doc(source).docAsUpsert(true));

            logger.debug("Updating task: {} of workflow: {} with {}", taskId, workflowId, source);
            elasticsearchClient.update(request, Map.class);
            long endTime = Instant.now().toEpochMilli();
            logger.debug(
                    "Time taken {} for updating task: {} of workflow: {}",
                    endTime - startTime,
                    taskId,
                    workflowId);
            Monitors.recordESIndexTime("update_task", "", endTime - startTime);
            Monitors.recordWorkerQueueSize(
                    "indexQueue", ((ThreadPoolExecutor) executorService).getQueue().size());
        } catch (Exception e) {
            logger.error("Failed to update task: {} of workflow: {}", taskId, workflowId, e);
            Monitors.error(className, "update");
        }
    }

    @Override
    public CompletableFuture<Void> asyncUpdateTask(
            String workflowId, String taskId, String[] keys, Object[] values) {
        return CompletableFuture.runAsync(
                () -> updateTask(workflowId, taskId, keys, values), executorService);
    }

    @Override
    public CompletableFuture<Void> asyncUpdateWorkflow(
            String workflowInstanceId, String[] keys, Object[] values) {
        return CompletableFuture.runAsync(
                () -> updateWorkflow(workflowInstanceId, keys, values), executorService);
    }

    @Override
    public String get(String workflowInstanceId, String fieldToGet) {
        try {
            GetResponse<Map> response =
                    elasticsearchClient.get(
                            g -> g.index(workflowIndexName).id(workflowInstanceId), Map.class);

            if (response.found()) {
                Map<String, Object> sourceAsMap = response.source();
                if (sourceAsMap.get(fieldToGet) != null) {
                    return sourceAsMap.get(fieldToGet).toString();
                }
            }

            logger.debug(
                    "Unable to find Workflow: {} in ElasticSearch index: {}.",
                    workflowInstanceId,
                    workflowIndexName);
        } catch (IOException e) {
            logger.error(
                    "Unable to get Workflow: {} from ElasticSearch index: {}",
                    workflowInstanceId,
                    workflowIndexName,
                    e);
        }
        return null;
    }

    private <T> SearchResult<T> searchObjects(
            String indexName,
            co.elastic.clients.elasticsearch._types.query_dsl.Query queryBuilder,
            int start,
            int size,
            List<String> sortOptions,
            boolean idOnly,
            Class<T> clazz)
            throws IOException {

        SearchRequest.Builder searchBuilder = new SearchRequest.Builder();
        searchBuilder.index(indexName).query(queryBuilder).from(start).size(size);

        if (idOnly) {
            searchBuilder.source(s -> s.fetch(false));
        }

        if (sortOptions != null && !sortOptions.isEmpty()) {
            // spotless:off
            for (String s : sortOptions) {
                String[] parts = s.split(":");
                String field = parts[0];
                co.elastic.clients.elasticsearch._types.SortOrder order =
                        (parts.length > 1)
                                ? co.elastic.clients.elasticsearch._types.SortOrder.valueOf(
                                        parts[1].toUpperCase())
                                : co.elastic.clients.elasticsearch._types.SortOrder.Asc;
                searchBuilder.sort(
                        co.elastic.clients.elasticsearch._types.SortOptions.of(
                                so -> so.field(f -> f.field(field).order(order))));
            }
            // spotless:on
        }

        SearchResponse<T> response = elasticsearchClient.search(searchBuilder.build(), clazz);
        return mapSearchResult(response, idOnly, clazz);
    }

    private <T> SearchResult<T> mapSearchResult(
            SearchResponse<T> response, boolean idOnly, Class<T> clazz) {
        long count = response.hits().total().value();
        List<T> result;
        if (idOnly) {
            result =
                    response.hits().hits().stream()
                            .map(hit -> clazz.cast(hit.id()))
                            .collect(Collectors.toList());
        } else {
            result =
                    response.hits().hits().stream()
                            .map(hit -> hit.source())
                            .collect(Collectors.toList());
        }
        return new SearchResult<>(count, result);
    }

    @Override
    public List<String> searchArchivableWorkflows(String indexName, long archiveTtlDays) {
        var archiveDate = LocalDate.now().minusDays(archiveTtlDays).toString();
        var prevArchiveDate = LocalDate.now().minusDays(archiveTtlDays + 1).toString();

        // spotless:off
        var rangeQuery = RangeQuery.of(r -> r.field("endTime").lt(JsonData.of(archiveDate)).gte(JsonData.of(prevArchiveDate)));
        var completed = TermQuery.of(t -> t.field("status").value(v -> v.stringValue("COMPLETED")));
        var failed = TermQuery.of(t -> t.field("status").value(v -> v.stringValue("FAILED")));
        var timedOut = TermQuery.of(t -> t.field("status").value(v -> v.stringValue("TIMED_OUT")));
        var terminated = TermQuery.of(t -> t.field("status").value(v -> v.stringValue("TERMINATED")));
        var archivedExists = ExistsQuery.of(e -> e.field("archived"));

        var qry = Query.of(q -> q.bool(b -> b
            .must(m -> m.range(rangeQuery))
            .should(s -> s.term(completed))
            .should(s -> s.term(failed))
            .should(s -> s.term(timedOut))
            .should(s -> s.term(terminated))
            .mustNot(mn -> mn.exists(archivedExists))
            .minimumShouldMatch("1")
        ));
        // spotless:on

        SearchResult<String> workflowIds;
        try {
            workflowIds = searchObjects(indexName, qry, 0, 1000, null, true, String.class);
        } catch (IOException e) {
            logger.error("Unable to communicate with ES to find archivable workflows", e);
            return Collections.emptyList();
        }

        return workflowIds.getResults();
    }

    @Override
    public long getWorkflowCount(String query, String freeText) {
        try {
            return getObjectCounts(query, freeText, WORKFLOW_DOC_TYPE);
        } catch (Exception e) {
            throw new NonTransientException(e.getMessage(), e);
        }
    }

    private long getObjectCounts(String structuredQuery, String freeTextQuery, String docType)
            throws ParserException, IOException {
        var queryBuilder = boolQueryBuilder(structuredQuery, freeTextQuery);

        String indexName = getIndexName(docType);
        CountRequest countRequest = CountRequest.of(c -> c.index(indexName).query(queryBuilder));
        CountResponse countResponse = elasticsearchClient.count(countRequest);
        return countResponse.count();
    }

    public List<String> searchRecentRunningWorkflows(
            int lastModifiedHoursAgoFrom, int lastModifiedHoursAgoTo) {
        // spotless:off
        Instant now = Instant.now();
        var gtTime = JsonData.of(now.minusSeconds(lastModifiedHoursAgoFrom * 3600L));
        var ltTime = JsonData.of(now.minusSeconds(lastModifiedHoursAgoTo * 3600L));

        var statusTerm = TermQuery.of(t -> t.field("status").value(v -> v.stringValue("RUNNING")));
        var gtRange = RangeQuery.of(r -> r.field("updateTime").gt(gtTime));
        var ltRange = RangeQuery.of(r -> r.field("updateTime").lt(ltTime));
        var qry = Query.of(q -> q.bool(
            b -> b.must(m -> m.range(gtRange))
                .must(m -> m.range(ltRange))
                .must(m -> m.term(statusTerm))
        ));
        // spotless:on

        SearchResult<String> workflowIds;
        try {
            workflowIds =
                    searchObjects(
                            workflowIndexName,
                            qry,
                            0,
                            5000,
                            Collections.singletonList("updateTime:ASC"),
                            true,
                            String.class);
        } catch (IOException e) {
            logger.error("Unable to communicate with ES to find recent running workflows", e);
            return Collections.emptyList();
        }

        return workflowIds.getResults();
    }

    private void indexObject(final String index, final String docType, final Object doc) {
        indexObject(index, docType, null, doc);
    }

    private void indexObject(
            final String index, final String docType, final String docId, final Object doc) {

        // spotless:off
        Map<String, Object> docMap;
        try {
            docMap = objectMapper.readValue(objectMapper.writeValueAsString(doc), Map.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        BulkOperation bulkOperation = BulkOperation.of(bo -> bo.index(ib -> {
            ib.index(index).id(docId).document(docMap);
            return ib;
        }));
        // spotless:on

        if (bulkRequests.get(docType) == null) {
            bulkRequests.put(
                    docType, new BulkRequests(System.currentTimeMillis(), new ArrayList<>()));
        }

        bulkRequests.get(docType).getBulkRequest().add(bulkOperation);
        if (bulkRequests.get(docType).getBulkRequest().size() >= this.indexBatchSize) {
            indexBulkRequest(docType);
        }
    }

    private synchronized void indexBulkRequest(String docType) {
        if (bulkRequests.get(docType).getBulkRequest() != null
                && bulkRequests.get(docType).getBulkRequest().size() > 0) {
            synchronized (bulkRequests.get(docType).getBulkRequest()) {
                indexWithRetry(
                        bulkRequests.get(docType).getBulkRequest(),
                        "Bulk Indexing " + docType,
                        docType);
                bulkRequests.put(
                        docType, new BulkRequests(System.currentTimeMillis(), new ArrayList<>()));
            }
        }
    }

    /**
     * Performs an index operation with a retry.
     *
     * @param request The index request that we want to perform.
     * @param operationDescription The type of operation that we are performing.
     */
    private void indexWithRetry(
            final List<BulkOperation> request, final String operationDescription, String docType) {
        try {
            long startTime = Instant.now().toEpochMilli();
            retryTemplate.execute(
                    context -> {
                        try {
                            return elasticsearchClient.bulk(b -> b.operations(request));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
            long endTime = Instant.now().toEpochMilli();
            logger.debug(
                    "Time taken {} for indexing object of type: {}", endTime - startTime, docType);
            Monitors.recordESIndexTime("index_object", docType, endTime - startTime);
            Monitors.recordWorkerQueueSize(
                    "indexQueue", ((ThreadPoolExecutor) executorService).getQueue().size());
            Monitors.recordWorkerQueueSize(
                    "logQueue", ((ThreadPoolExecutor) logExecutorService).getQueue().size());
        } catch (Exception e) {
            Monitors.error(className, "index");
            logger.error("Failed to index {} for request type: {}", request, docType, e);
        }
    }

    /**
     * Flush the buffers if bulk requests have not been indexed for the past {@link
     * ElasticSearchProperties#getAsyncBufferFlushTimeout()} seconds This is to prevent data loss in
     * case the instance is terminated, while the buffer still holds documents to be indexed.
     */
    private void flushBulkRequests() {
        bulkRequests.entrySet().stream()
                .filter(
                        entry ->
                                (System.currentTimeMillis() - entry.getValue().getLastFlushTime())
                                        >= asyncBufferFlushTimeout * 1000L)
                .filter(
                        entry ->
                                entry.getValue().getBulkRequest() != null
                                        && entry.getValue().getBulkRequest().size() > 0)
                .forEach(
                        entry -> {
                            logger.debug(
                                    "Flushing bulk request buffer for type {}, size: {}",
                                    entry.getKey(),
                                    entry.getValue().getBulkRequest().size());
                            indexBulkRequest(entry.getKey());
                        });
    }

    private static class BulkRequests {

        private final long lastFlushTime;
        private final List<BulkOperation> bulkRequest;

        long getLastFlushTime() {
            return lastFlushTime;
        }

        List<BulkOperation> getBulkRequest() {
            return bulkRequest;
        }

        BulkRequests(long lastFlushTime, List<BulkOperation> bulkRequest) {
            this.lastFlushTime = lastFlushTime;
            this.bulkRequest = bulkRequest;
        }
    }
}
