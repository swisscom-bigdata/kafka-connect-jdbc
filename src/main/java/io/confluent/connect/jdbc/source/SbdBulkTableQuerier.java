package io.confluent.connect.jdbc.source;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.mbean.BatchMetrics;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SbdBulkTableQuerier extends BulkTableQuerier {

  /**
   * UUID of the batch
   */
  public static final String HEADER_BATCH_ID = "sbd.batch.id";

  /**
   * RFC 3339 datetime associated to this batch.
   * This is the expected time for this batch.
   * If the batch is retried 3 times, then the same batch time will be used for all the attempts.
   */
  public static final String HEADER_BATCH_TIME = "sbd.batch.time";

  /**
   * Size of the batch, representing the number of records in the ResultSet
   */
  public static final String HEADER_BATCH_SIZE = "sbd.batch.size";

  /**
   * Boolean indicating if this batch is completed or not.
   * Value is always FALSE, except for the last record where value will be TRUE.
   */
  public static final String HEADER_BATCH_COMPLETED = "sbd.batch.completed";

  /**
   * Index of the record in the batch.
   * First item has value=1, last item has value=HEADER_BATCH_SIZE
   */
  public static final String HEADER_BATCH_INDEX = "sbd.batch.index";

  /**
   * RFC 3339 datetime when the batch started.
   * At the first attempt, HEADER_BATCH_STARTED_AT equals HEADER_BATCH_TIME
   */
  public static final String HEADER_BATCH_STARTED_AT = "sbd.batch.started.at";

  /**
   * RFC 3339 datetime when the batch completed.
   */
  public static final String HEADER_BATCH_COMPLETED_AT = "sbd.batch.completed.at";

  private static final String THREAD_NAME_PREFIX = "task-thread-";
  private static final Logger log = LoggerFactory.getLogger(SbdBulkTableQuerier.class);

  private final BatchMetrics metrics;
  private String currentBatchId;
  private Instant currentBatchTime;
  private Instant currentBatchStartTime;
  private Time time;

  public SbdBulkTableQuerier(
      DatabaseDialect dialect,
      QueryMode mode,
      String name,
      String topicPrefix,
      String suffix,
      Map<String, Object> offset,
      Time time
  ) {
    super(dialect, mode, name, topicPrefix, suffix);

    // register SBD MBeans
    String taskId = Thread.currentThread().getName().substring(THREAD_NAME_PREFIX.length());
    metrics = new BatchMetrics(taskId, name);
    this.time = time;

    currentBatchId = null;
    currentBatchTime = null;
    if (offset.containsKey(HEADER_BATCH_ID)) {
      String batchId = (String) offset.get(HEADER_BATCH_ID);
      boolean batchCompleted = (boolean) offset.get(HEADER_BATCH_COMPLETED);
      Instant batchTime = Instant.parse((String) offset.get(HEADER_BATCH_TIME));

      if (batchCompleted) {
        lastUpdate = batchTime.toEpochMilli();
      } else {
        // if the batch is not completed we assume all the previous ones are completed or aborted
        // so we will only retry the current one if needed
        currentBatchId = batchId;
        currentBatchTime = batchTime;
      }
    }

    if (currentBatchId != null) {
      log.info("Configured to retry batch.id={}, batch.time={}", currentBatchId, currentBatchTime);
    }
  }

  public long getLastUpdate() {
    return lastUpdate;
  }

  protected ResultSet executeQuery() throws SQLException {
    return stmt.executeQuery();
  }

  @Override
  public SourceRecord extractRecord() throws SQLException {
    SourceRecord sr = super.extractRecord();

    final int batchSize = resultSet.getFetchSize();
    final int rowIndex = resultSet.getRow(); // 1 for first row, 2 for second one
    final boolean isLastRecord = rowIndex == batchSize;

    // we retry failing batches during 3h but not if we are after the next scheduled execution time
    if (rowIndex == 1
        && Instant.ofEpochMilli(time.milliseconds()).minus(3, ChronoUnit.HOURS).isAfter(Instant.ofEpochMilli(lastUpdate))
        && Instant.ofEpochMilli(time.milliseconds()).isBefore(Instant.ofEpochMilli(nextExecutionTime))) {
      currentBatchId = null;
    }

    // new batch
    if (currentBatchId == null) {
      currentBatchId = UUID.randomUUID().toString();
      currentBatchTime = Instant.ofEpochMilli(currentExecutionTime);
      currentBatchStartTime = currentBatchTime;
    }

    // batch already defined, here it's a new attempt
    else if (currentBatchStartTime == null) {
      currentBatchStartTime = Instant.now();
    }

    Headers headers = new ConnectHeaders(sr.headers());
    headers.add(HEADER_BATCH_ID, new SchemaAndValue(Schema.STRING_SCHEMA, currentBatchId));
    headers.add(HEADER_BATCH_TIME, new SchemaAndValue(Schema.STRING_SCHEMA, currentBatchTime));
    headers.add(HEADER_BATCH_SIZE, new SchemaAndValue(Schema.INT32_SCHEMA, batchSize));
    headers.add(HEADER_BATCH_INDEX, new SchemaAndValue(Schema.INT32_SCHEMA, rowIndex));
    headers.add(HEADER_BATCH_STARTED_AT, new SchemaAndValue(Schema.STRING_SCHEMA, currentBatchStartTime));
    headers.add(HEADER_BATCH_COMPLETED, new SchemaAndValue(Schema.BOOLEAN_SCHEMA, isLastRecord));

    if (rowIndex == batchSize) {
      final Instant batchEndTime = Instant.now();
      headers.add(HEADER_BATCH_COMPLETED_AT, new SchemaAndValue(Schema.STRING_SCHEMA, batchEndTime));
    }

    Map<String, Object> sourceOffset = new HashMap<>(sr.sourceOffset());
    sourceOffset.put(HEADER_BATCH_ID, currentBatchId);
    sourceOffset.put(HEADER_BATCH_TIME, currentBatchTime);
    sourceOffset.put(HEADER_BATCH_COMPLETED, isLastRecord);

    recordBatchMetrics(currentBatchId, batchSize, rowIndex);

    return new SourceRecord(sr.sourcePartition(), sourceOffset, sr.topic(),
        sr.kafkaPartition(), sr.keySchema(), sr.key(), sr.valueSchema(),
        sr.value(), sr.timestamp(), headers);
  }

  @Override
  public void reset(long now) {
    super.reset(now); // set lastUpdate = now

    currentBatchId = null;
    currentBatchTime = null;
    currentBatchStartTime = null;
  }

  private void recordBatchMetrics(String batchId, int batchSize, int rowIndex) {
    metrics.recordSize(batchId, batchSize);
    metrics.recordPosition(batchId, rowIndex);
  }
}
