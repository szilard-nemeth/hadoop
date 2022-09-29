package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.calculations;

public enum CSCalculationType {
  USER_AM_RESOURCE_PER_PARTITION("UserAMResourceLimitPerPartition");

  private String name;

  CSCalculationType(String name) {
    this.name = name;
  }
}
