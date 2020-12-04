/*
 * Copyright  Confluent Inc.?
 */

package io.confluent.connect.jdbc.mbean;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;

public class BatchMetrics implements Closeable {
  private final String taskId;

  private Map<String, BatchMetric> batchMetrics = new HashMap<>();

  public BatchMetrics(String taskId) {
    this.taskId = taskId;
  }

  public void recordSize(String batchId, int size) {
    BatchMetric batchMetric = batchMetrics.computeIfAbsent(
        batchId,
        s -> new BatchMetric(taskId, batchId));
    batchMetric.setSize(size);
    batchMetrics.put(batchId, batchMetric);
  }

  public void recordPosition(String batchId, int position) {
    BatchMetric batchMetric = batchMetrics.computeIfAbsent(
        batchId,
        s -> new BatchMetric(taskId, batchId));
    batchMetric.setPosition(position);
    batchMetrics.put(batchId, batchMetric);
  }

  @Override
  public void close() {
    batchMetrics.values().forEach(e -> e.close());
  }

  // only used for development, todo delete
  public static void main(String[] args) throws InterruptedException {
    BatchMetrics met = new BatchMetrics("abc-blah-blah-1");
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
