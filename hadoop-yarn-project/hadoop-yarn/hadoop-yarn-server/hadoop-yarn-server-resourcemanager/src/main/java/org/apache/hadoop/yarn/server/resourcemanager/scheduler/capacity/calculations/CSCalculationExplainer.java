package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.calculations;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.UsersManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CSCalculationExplainer {
  private static final Logger LOG = LoggerFactory.getLogger(CSCalculationExplainer.class);
  private final CSCalculationType type;
  protected CalculationContext ctx;
  public boolean explain;

  public CSCalculationExplainer(CSCalculationType type, boolean explain) {
    this.type = type;
    this.explain = explain;
  }

  public void log(String name, float value) {
    if (explain) {
      LOG.debug("Interim calculation: {}: {}", name, value);
    }
  }

  public void log(String name, Object value) {
    if (explain) {
      LOG.debug("Interim calculation: {}: {}", name, value);
    }
  }
  
  public void log(String name, String alias, Object value) {
    if (explain) {
      LOG.debug("Interim calculation: Name: {}, Alias: {}, Value: {}", name, alias, value);
    }
  }
}
