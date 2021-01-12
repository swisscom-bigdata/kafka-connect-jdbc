/*
 * Copyright  Confluent Inc.?
 */

package io.confluent.connect.jdbc.mbean;

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import java.io.Closeable;
import java.lang.management.ManagementFactory;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Not thread safe.
 */
public class BatchMetric implements BatchMetricMBean, Closeable {

  private static final Logger log = LoggerFactory.getLogger(BatchMetric.class);
  private static final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
  private static final String DOMAIN = "kafka.connect";

  private final ObjectName name;

  private Integer size;
  private Integer position;

  @Override
  public Integer getSize() {
    return size;
  }

  public void setSize(Integer size) {
    this.size = size;
  }

  @Override
  public Integer getPosition() {
    return position;
  }

  public void setPosition(Integer position) {
    this.position = position;
  }

  public BatchMetric(String taskId, String tableName, String batchId) {
    log.info("Create BatchMetric with taskId={} tableName={} batchId={}",
        taskId, tableName, batchId);
    try {
      Map<String, String> keyValues = new LinkedHashMap<>();
      keyValues.put("type", "source-task-metrics");
      keyValues.put("extension", "sbd");
      keyValues.put("task", taskId);
      keyValues.put("table", tableName);
      keyValues.put("batch", batchId);

      // note that we are not escaping/encoding the arguments, as we don't expect
      String n = DOMAIN + ":" + keyValues.entrySet().stream()
          .map(e -> e.getKey() + "=" + e.getValue())
          .collect(Collectors.joining(","));
      log.debug("ObjectName={}", n);

      name = ObjectName.getInstance(n);
      mbs.registerMBean(this, name);
    } catch (MalformedObjectNameException | NotCompliantMBeanException
        | InstanceAlreadyExistsException | MBeanRegistrationException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    try {
      mbs.unregisterMBean(name);
    } catch (InstanceNotFoundException | MBeanRegistrationException e) {
      log.error("Error closing BatchMetric for {}", name, e);
    }
  }
}
