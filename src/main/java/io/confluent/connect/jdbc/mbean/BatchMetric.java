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

/**
 * Not thread safe.
 */
public class BatchMetric implements BatchMetricMBean, Closeable {

  private static final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
  private static final String DOMAIN = "sbd.jdbc";

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

  public BatchMetric(String taskId, String batchId) {
    try {
      Map<String, String> keyValues = new LinkedHashMap<>();
      keyValues.put("type", "BatchMetric");
      keyValues.put("task", taskId);
      keyValues.put("batch", batchId);

      // note that we are not escaping/encoding the arguments, as we don't expect

      name = ObjectName.getInstance(DOMAIN + ":" + keyValues.entrySet().stream()
          .map(e -> e.getKey() + "=" + e.getValue())
          .collect(Collectors.joining(",")));
      mbs.registerMBean(this, name);
    } catch (MalformedObjectNameException | NotCompliantMBeanException
        | InstanceAlreadyExistsException | MBeanRegistrationException e) {
      // todo
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {  // todo when call? a few minutes after batch is completed?
    try {
      mbs.unregisterMBean(name);
    } catch (InstanceNotFoundException | MBeanRegistrationException e) {
      // todo
      e.printStackTrace();
    }
  }
}
