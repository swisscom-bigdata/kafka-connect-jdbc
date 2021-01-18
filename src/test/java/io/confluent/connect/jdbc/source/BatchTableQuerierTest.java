package io.confluent.connect.jdbc.source;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class BatchTableQuerierTest extends JdbcSourceTaskTestBase {

  @Before
  public void setup() throws Exception {
    super.setup();

    Map<String, String> props = new HashMap<>();
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, db.getUrl());
    props.put(JdbcSourceTaskConfig.TABLES_CONFIG, SINGLE_TABLE_NAME);
    props.put(JdbcSourceTaskConfig.BATCH_MAX_ROWS_CONFIG, "1");
    props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
    props.put(JdbcSourceConnectorConfig.POLL_INTERVAL_MODE_CONFIG, JdbcSourceConnectorConfig.POLL_INTERVAL_MODE_CRON);
    props.put(JdbcSourceConnectorConfig.POLL_INTERVAL_CRON_CONFIG, "0 0 0/1 ? * * *"); // every hour starting at 00:00
    props.put(JdbcSourceTaskConfig.TOPIC_PREFIX_CONFIG, TOPIC_PREFIX);

    task.start(props);
  }

  @After
  public void tearDown() throws Exception {
    task.stop();
    super.tearDown();
  }

  @Test
  public void testBatchHeaders() throws Exception {
    db.createTable(SINGLE_TABLE_NAME, "id", "VARCHAR(1) NOT NULL");
    db.insert(SINGLE_TABLE_NAME, "id", "a");
    db.insert(SINGLE_TABLE_NAME, "id", "b");
    db.insert(SINGLE_TABLE_NAME, "id", "c");

    Instant poll1Time = Instant
        .ofEpochMilli(time.milliseconds())
        .truncatedTo(ChronoUnit.HOURS)
        .plus(1, ChronoUnit.HOURS);

    // Polling record id=a
    List<SourceRecord> records = task.poll();
    assertEquals(1, records.size());
    String batchId = (String) records.get(0).sourceOffset().get(
        BatchTableQuerier.HEADER_BATCH_ID);

    Map<String, String> sourcePartition = new HashMap<String, String>();
    sourcePartition.put("table", "test");

    Map<String, Object> sourceOffset12 = new HashMap<String, Object>();
    sourceOffset12.put("batch.id", batchId);
    sourceOffset12.put("batch.time", poll1Time.toString());
    sourceOffset12.put("batch.completed", false);

    Map<String, Object> sourceOffset3 = new HashMap<String, Object>();
    sourceOffset3.put("batch.id", batchId);
    sourceOffset3.put("batch.time", poll1Time.toString());
    sourceOffset3.put("batch.completed", true);

    SchemaBuilder sb = new SchemaBuilder(Type.STRUCT);
    sb.name("test");
    sb.field("id", Schema.STRING_SCHEMA);
    Schema schema = sb.schema();

    Struct value1 = new Struct(schema);
    Struct value2 = new Struct(schema);
    Struct value3 = new Struct(schema);
    value1.put("id", "a");
    value2.put("id", "b");
    value3.put("id", "c");

    Headers headers1 = new ConnectHeaders();
    headers1.add(BatchTableQuerier.HEADER_BATCH_ID, new SchemaAndValue(Schema.STRING_SCHEMA, batchId));
    headers1.add(BatchTableQuerier.HEADER_BATCH_TIME, new SchemaAndValue(Schema.STRING_SCHEMA, poll1Time.toString()));
    headers1.add(BatchTableQuerier.HEADER_BATCH_SIZE, new SchemaAndValue(Schema.INT32_SCHEMA, 3));
    headers1.add(BatchTableQuerier.HEADER_BATCH_INDEX, new SchemaAndValue(Schema.INT32_SCHEMA, 1));
    headers1.add(BatchTableQuerier.HEADER_BATCH_STARTED_AT, new SchemaAndValue(Schema.STRING_SCHEMA, poll1Time.toString()));
    headers1.add(BatchTableQuerier.HEADER_BATCH_COMPLETED, new SchemaAndValue(Schema.BOOLEAN_SCHEMA, false));

    SourceRecord r1 = new SourceRecord(sourcePartition, sourceOffset12, "test-test",
        null, null, null, schema, value1, null, headers1);
    assertEquals(r1, records.get(0));

    time.sleep(5000);
    Instant poll2Time = poll1Time.plusSeconds(5);

    Headers headers2 = new ConnectHeaders();
    headers2.add(BatchTableQuerier.HEADER_BATCH_ID, new SchemaAndValue(Schema.STRING_SCHEMA, batchId));
    headers2.add(BatchTableQuerier.HEADER_BATCH_TIME, new SchemaAndValue(Schema.STRING_SCHEMA, poll1Time.toString()));
    headers2.add(BatchTableQuerier.HEADER_BATCH_SIZE, new SchemaAndValue(Schema.INT32_SCHEMA, 3));
    headers2.add(BatchTableQuerier.HEADER_BATCH_INDEX, new SchemaAndValue(Schema.INT32_SCHEMA, 2));
    headers2.add(BatchTableQuerier.HEADER_BATCH_STARTED_AT, new SchemaAndValue(Schema.STRING_SCHEMA, poll1Time.toString()));
    headers2.add(BatchTableQuerier.HEADER_BATCH_COMPLETED, new SchemaAndValue(Schema.BOOLEAN_SCHEMA, false));

    // Polling record id=b
    records = task.poll();
    assertEquals(1, records.size());
    SourceRecord r2 = new SourceRecord(sourcePartition, sourceOffset12, "test-test",
        null, null, null, schema, value2, null, headers2);
    assertEquals(r2, records.get(0));

    time.sleep(5000);
    Instant poll3Time = poll2Time.plusSeconds(5);

    Headers headers3 = new ConnectHeaders();
    headers3.add(BatchTableQuerier.HEADER_BATCH_ID, new SchemaAndValue(Schema.STRING_SCHEMA, batchId));
    headers3.add(BatchTableQuerier.HEADER_BATCH_TIME, new SchemaAndValue(Schema.STRING_SCHEMA, poll1Time.toString()));
    headers3.add(BatchTableQuerier.HEADER_BATCH_SIZE, new SchemaAndValue(Schema.INT32_SCHEMA, 3));
    headers3.add(BatchTableQuerier.HEADER_BATCH_INDEX, new SchemaAndValue(Schema.INT32_SCHEMA, 3));
    headers3.add(BatchTableQuerier.HEADER_BATCH_STARTED_AT, new SchemaAndValue(Schema.STRING_SCHEMA, poll1Time.toString()));
    headers3.add(BatchTableQuerier.HEADER_BATCH_COMPLETED, new SchemaAndValue(Schema.BOOLEAN_SCHEMA, true));
    headers3.add(BatchTableQuerier.HEADER_BATCH_COMPLETED_AT, new SchemaAndValue(Schema.STRING_SCHEMA, poll3Time.toString()));

    // Polling record id=c
    records = task.poll();
    assertEquals(1, records.size());
    SourceRecord r3 = new SourceRecord(sourcePartition, sourceOffset3, "test-test",
        null, null, null, schema, value3, null, headers3);
    assertEquals(r3, records.get(0));

    // If we poll more, then we will pull the next batch
    records = task.poll();
    assertNotEquals(batchId, records.get(0).headers().lastWithName(
        BatchTableQuerier.HEADER_BATCH_ID).value());
    assertEquals(poll1Time.plus(1, ChronoUnit.HOURS).toString(), records.get(0).headers().lastWithName(
        BatchTableQuerier.HEADER_BATCH_TIME).value());
  }
}
