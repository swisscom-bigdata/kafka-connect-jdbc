/*
 * Copyright 2021 Confluent Inc.
 * Copyright 2021 Swisscom (Switzerland) Ltd
 */

package io.confluent.connect.jdbc.mbean;

public interface BatchMetricMBean {
  Integer getSize();

  Integer getPosition();
}
