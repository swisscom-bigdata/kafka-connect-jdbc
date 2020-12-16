/*
 * Copyright  Confluent Inc.?
 */

package io.confluent.connect.jdbc.mbean;

import java.io.Closeable;
import java.util.LinkedHashMap;
import java.util.Map;

public class BatchMetrics implements Closeable {
  private final String taskId;
  private final String tableName;

  private Map<String, BatchMetric> batchMetrics = new LinkedHashMap<String, BatchMetric>(){
    @Override
    protected boolean removeEldestEntry(Map.Entry<String, BatchMetric> eldest) {
      if (size() > 5) {
        eldest.getValue().close();
        return true;
      }
      return false;
    }
  };

  public BatchMetrics(String taskId, String tableName) {
    this.taskId = taskId;
    this.tableName = tableName;
  }

  public void recordSize(String batchId, int size) {
    BatchMetric batchMetric = batchMetrics.computeIfAbsent(
        batchId,
        s -> new BatchMetric(taskId, tableName, batchId));
    batchMetric.setSize(size);
    batchMetrics.put(batchId, batchMetric);
  }

  public void recordPosition(String batchId, int position) {
    BatchMetric batchMetric = batchMetrics.computeIfAbsent(
        batchId,
        s -> new BatchMetric(taskId, tableName, batchId));
    batchMetric.setPosition(position);
    batchMetrics.put(batchId, batchMetric);
  }

  @Override
  public void close() {
    batchMetrics.values().forEach(BatchMetric::close);
  }

  // only used for development, todo delete
  public static void main(String[] args) throws InterruptedException {
    BatchMetrics met = new BatchMetrics("abc-blah-blah-1", "table");
    met.recordSize("A", 100);
    met.recordPosition("A", 50);
    met.recordSize("B", 200);

    for (int i = 0; i < 200000; i++) {
      met.recordPosition("B", i);
      System.out.println(i);
      Thread.sleep(5_000L);
    }
    met.close();
  }

}
