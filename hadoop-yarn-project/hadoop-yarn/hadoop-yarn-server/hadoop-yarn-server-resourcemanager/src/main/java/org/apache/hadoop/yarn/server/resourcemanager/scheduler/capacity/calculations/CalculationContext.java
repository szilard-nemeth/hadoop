package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.calculations;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractCSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractLeafQueue;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class CalculationContext {
  AbstractLeafQueue queue;
  ReentrantReadWriteLock.ReadLock readLock;

  public CalculationContext(AbstractLeafQueue abstractLeafQueue,
      ReentrantReadWriteLock.ReadLock readLock) {
    this.queue = abstractLeafQueue;
    this.readLock = readLock;
  }
}
