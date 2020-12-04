/*
 * Copyright  Confluent Inc.?
 */

package io.confluent.connect.jdbc.mbean;

public interface BatchMetricMBean {
  Integer getSize();

  Integer getPosition();
}
