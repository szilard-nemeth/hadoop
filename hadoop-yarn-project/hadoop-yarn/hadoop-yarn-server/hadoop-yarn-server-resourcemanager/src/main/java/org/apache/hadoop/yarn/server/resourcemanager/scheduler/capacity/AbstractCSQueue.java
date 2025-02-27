/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueConfigurations;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueStatistics;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.AccessRequest;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.security.PrivilegedEntity;
import org.apache.hadoop.yarn.security.PrivilegedEntity.EntityType;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueResourceQuotas;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.AbsoluteResourceType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ContainerAllocationProposal;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ResourceCommitRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.SchedulerContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.SimpleCandidateNodeSet;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.DOT;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.UNDEFINED;

public abstract class AbstractCSQueue implements CSQueue {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractCSQueue.class);
  volatile CSQueue parent;
  final String queueName;
  private final String queuePath;
  volatile int numContainers;

  final Resource minimumAllocation;
  volatile Resource maximumAllocation;
  private volatile QueueState state = null;
  final CSQueueMetrics metrics;
  protected final PrivilegedEntity queueEntity;

  final ResourceCalculator resourceCalculator;
  Set<String> accessibleLabels;
  protected Set<String> configuredNodeLabels;
  Set<String> resourceTypes;
  final RMNodeLabelsManager labelManager;
  String defaultLabelExpression;
  private String multiNodeSortingPolicyName = null;

  Map<AccessType, AccessControlList> acls =
      new HashMap<AccessType, AccessControlList>();
  volatile boolean reservationsContinueLooking;
  private volatile boolean preemptionDisabled;
  // Indicates if the in-queue preemption setting is ever disabled within the
  // hierarchy of this queue.
  private boolean intraQueuePreemptionDisabledInHierarchy;

  // Track resource usage-by-label like used-resource/pending-resource, etc.
  volatile ResourceUsage queueUsage;

  // Track capacities like
  // used-capacity/abs-used-capacity/capacity/abs-capacity,
  // etc.
  QueueCapacities queueCapacities;

  QueueResourceQuotas queueResourceQuotas;

  // -1 indicates lifetime is disabled
  private volatile long maxApplicationLifetime = -1;

  private volatile long defaultApplicationLifetime = -1;

  // Indicates if this queue's default lifetime was set by a config property,
  // either at this level or anywhere in the queue's hierarchy.
  private volatile boolean defaultAppLifetimeWasSpecifiedInConfig = false;

  public enum CapacityConfigType {
    // FIXME, from what I can see, Percentage mode can almost apply to weighted
    // and percentage mode at the same time, there's only small area need to be
    // changed, we need to rename "PERCENTAGE" to "PERCENTAGE" and "WEIGHT"
    NONE, PERCENTAGE, ABSOLUTE_RESOURCE
  };
  protected CapacityConfigType capacityConfigType =
      CapacityConfigType.NONE;

  private final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
  protected CapacitySchedulerContext csContext;
  protected YarnAuthorizationProvider authorizer = null;

  protected ActivitiesManager activitiesManager;

  protected ReentrantReadWriteLock.ReadLock readLock;
  protected ReentrantReadWriteLock.WriteLock writeLock;

  volatile Priority priority = Priority.newInstance(0);
  private Map<String, Float> userWeights = new HashMap<String, Float>();
  private int maxParallelApps;

  // is it a dynamic queue?
  private boolean dynamicQueue = false;

  // The timestamp of the last submitted application to this queue.
  // Only applies to dynamic queues.
  private long lastSubmittedTimestamp;

  public AbstractCSQueue(CapacitySchedulerContext cs,
      String queueName, CSQueue parent, CSQueue old) throws IOException {
    this(cs, cs.getConfiguration(), queueName, parent, old);
  }

  public AbstractCSQueue(CapacitySchedulerContext cs,
      CapacitySchedulerConfiguration configuration, String queueName,
      CSQueue parent, CSQueue old) {

    this.labelManager = cs.getRMContext().getNodeLabelManager();
    this.parent = parent;
    this.queueName = queueName;
    this.queuePath = ((parent == null) ? "" : (parent.getQueuePath() + "."))
        + this.queueName;
    this.resourceCalculator = cs.getResourceCalculator();
    this.activitiesManager = cs.getActivitiesManager();

    // must be called after parent and queueName is set
    this.metrics = old != null ?
        (CSQueueMetrics) old.getMetrics() :
        CSQueueMetrics.forQueue(getQueuePath(), parent,
            cs.getConfiguration().getEnableUserMetrics(), configuration);

    this.csContext = cs;
    this.minimumAllocation = csContext.getMinimumResourceCapability();

    // initialize ResourceUsage
    queueUsage = new ResourceUsage();
    queueEntity = new PrivilegedEntity(EntityType.QUEUE, getQueuePath());

    // initialize QueueCapacities
    queueCapacities = new QueueCapacities(parent == null);

    // initialize queueResourceQuotas
    queueResourceQuotas = new QueueResourceQuotas();

    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  @VisibleForTesting
  protected void setupConfigurableCapacities() {
    setupConfigurableCapacities(csContext.getConfiguration());
  }

  protected void setupConfigurableCapacities(
      CapacitySchedulerConfiguration configuration) {
    CSQueueUtils.loadCapacitiesByLabelsFromConf(getQueuePath(), queueCapacities,
        configuration, configuredNodeLabels);
  }

  @Override
  public String getQueuePath() {
    return queuePath;
  }

  @Override
  public float getCapacity() {
    return queueCapacities.getCapacity();
  }

  @Override
  public float getAbsoluteCapacity() {
    return queueCapacities.getAbsoluteCapacity();
  }

  @Override
  public float getAbsoluteMaximumCapacity() {
    return queueCapacities.getAbsoluteMaximumCapacity();
  }

  @Override
  public float getAbsoluteUsedCapacity() {
    return queueCapacities.getAbsoluteUsedCapacity();
  }

  @Override
  public float getMaximumCapacity() {
    return queueCapacities.getMaximumCapacity();
  }

  @Override
  public float getUsedCapacity() {
    return queueCapacities.getUsedCapacity();
  }

  @Override
  public Resource getUsedResources() {
    return queueUsage.getUsed();
  }

  public int getNumContainers() {
    return numContainers;
  }

  @Override
  public QueueState getState() {
    return state;
  }

  @Override
  public CSQueueMetrics getMetrics() {
    return metrics;
  }

  @Override
  public String getQueueShortName() {
    return queueName;
  }

  @Override
  public String getQueueName() {
    return queueName;
  }

  @Override
  public PrivilegedEntity getPrivilegedEntity() {
    return queueEntity;
  }

  @Override
  public CSQueue getParent() {
    return parent;
  }

  @Override
  public void setParent(CSQueue newParentQueue) {
    this.parent = newParentQueue;
  }

  public Set<String> getAccessibleNodeLabels() {
    return accessibleLabels;
  }

  @Override
  public boolean hasAccess(QueueACL acl, UserGroupInformation user) {
    return authorizer.checkPermission(
        new AccessRequest(queueEntity, user, SchedulerUtils.toAccessType(acl),
            null, null, Server.getRemoteAddress(), null));
  }

  /**
   * Set maximum capacity - used only for testing.
   * @param maximumCapacity new max capacity
   */
  void setMaxCapacity(float maximumCapacity) {
    writeLock.lock();
    try {
      // Sanity check
      CSQueueUtils.checkMaxCapacity(getQueuePath(),
          queueCapacities.getCapacity(), maximumCapacity);
      float absMaxCapacity = CSQueueUtils.computeAbsoluteMaximumCapacity(
          maximumCapacity, parent);
      CSQueueUtils.checkAbsoluteCapacity(getQueuePath(),
          queueCapacities.getAbsoluteCapacity(), absMaxCapacity);

      queueCapacities.setMaximumCapacity(maximumCapacity);
      queueCapacities.setAbsoluteMaximumCapacity(absMaxCapacity);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Set maximum capacity
   * @param maximumCapacity new max capacity
   */
  void setMaxCapacity(String nodeLabel, float maximumCapacity) {
    writeLock.lock();
    try {
      // Sanity check
      CSQueueUtils.checkMaxCapacity(getQueuePath(),
          queueCapacities.getCapacity(nodeLabel), maximumCapacity);
      float absMaxCapacity = CSQueueUtils.computeAbsoluteMaximumCapacity(
          maximumCapacity, parent);
      CSQueueUtils.checkAbsoluteCapacity(getQueuePath(),
          queueCapacities.getAbsoluteCapacity(nodeLabel), absMaxCapacity);

      queueCapacities.setMaximumCapacity(maximumCapacity);
      queueCapacities.setAbsoluteMaximumCapacity(absMaxCapacity);
    } finally {
      writeLock.unlock();
    }
  }


  @Override
  public String getDefaultNodeLabelExpression() {
    return defaultLabelExpression;
  }

  protected void setupQueueConfigs(Resource clusterResource,
      CapacitySchedulerConfiguration configuration) throws
      IOException {

    writeLock.lock();
    try {
      if (isDynamicQueue() || this instanceof AbstractAutoCreatedLeafQueue) {
        setDynamicQueueProperties(configuration);
      }

      // Collect and set the Node label configuration
      initializeNodeLabels(configuration);

      // Initialize the queue capacities
      setupConfigurableCapacities(configuration);
      updateAbsoluteCapacities();

      updateCapacityConfigType();

      // Fetch minimum/maximum resource limits for this queue if
      // configured
      this.resourceTypes = new HashSet<>();
      for (AbsoluteResourceType type : AbsoluteResourceType.values()) {
        resourceTypes.add(type.toString().toLowerCase());
      }
      updateConfigurableResourceLimits(clusterResource);

      // Setup queue's maximumAllocation respecting the global
      // and the queue settings
      setupMaximumAllocation(configuration);

      // Initialize the queue state based on previous state, configured state
      // and its parent state
      initializeQueueState(configuration);

      authorizer = YarnAuthorizationProvider.getInstance(configuration);

      this.acls = configuration.getAcls(getQueuePath());

      this.userWeights = getUserWeightsFromHierarchy(configuration);

      this.reservationsContinueLooking =
          configuration.getReservationContinueLook();

      // Update metrics
      CSQueueUtils.updateQueueStatistics(resourceCalculator, clusterResource,
          this, labelManager, null);

      // Store preemption settings
      this.preemptionDisabled = isQueueHierarchyPreemptionDisabled(this,
          configuration);
      this.intraQueuePreemptionDisabledInHierarchy =
          isIntraQueueHierarchyPreemptionDisabled(this, configuration);
      this.priority = configuration.getQueuePriority(
          getQueuePath());

      // Update multi-node sorting algorithm for scheduling as configured.
      setMultiNodeSortingPolicyName(
          configuration.getMultiNodesSortingAlgorithmPolicy(getQueuePath()));

      // Setup application related limits
      setupApplicationLimits(configuration);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Set properties specific to dynamic queues.
   * @param configuration configuration on which the properties are set
   */
  protected void setDynamicQueueProperties(
      CapacitySchedulerConfiguration configuration) {
    // Set properties from parent template
    if (getParent() instanceof ParentQueue) {
      ((ParentQueue) getParent()).getAutoCreatedQueueTemplate()
          .setTemplateEntriesForChild(configuration, getQueuePath());

      String parentTemplate = String.format("%s.%s", getParent().getQueuePath(),
          AutoCreatedQueueTemplate.AUTO_QUEUE_TEMPLATE_PREFIX);
      parentTemplate = parentTemplate.substring(0, parentTemplate.lastIndexOf(
          DOT));
      Set<String> parentNodeLabels = csContext
          .getCapacitySchedulerQueueManager().getConfiguredNodeLabels()
          .getLabelsByQueue(parentTemplate);

      if (parentNodeLabels != null && parentNodeLabels.size() > 1) {
        csContext.getCapacitySchedulerQueueManager().getConfiguredNodeLabels()
            .setLabelsByQueue(queuePath, new HashSet<>(parentNodeLabels));
      }
    }
  }

  private void initializeNodeLabels(
      CapacitySchedulerConfiguration configuration) throws IOException {
    // Collect and store labels
    this.accessibleLabels =
        configuration.getAccessibleNodeLabels(getQueuePath());
    this.defaultLabelExpression =
        configuration.getDefaultNodeLabelExpression(
            getQueuePath());

    // Inherit labels from parent if not set
    if (this.accessibleLabels == null && parent != null) {
      this.accessibleLabels = parent.getAccessibleNodeLabels();
    }

    // If the accessible labels is not null and the queue has a parent with a
    // similar set of labels copy the defaultNodeLabelExpression from the parent
    if (this.accessibleLabels != null && parent != null
        && this.defaultLabelExpression == null &&
        this.accessibleLabels.containsAll(parent.getAccessibleNodeLabels())) {
      this.defaultLabelExpression = parent.getDefaultNodeLabelExpression();
    }

    if (csContext.getCapacitySchedulerQueueManager() != null
        && csContext.getCapacitySchedulerQueueManager()
        .getConfiguredNodeLabels() != null) {
      this.configuredNodeLabels = csContext.getCapacitySchedulerQueueManager()
          .getConfiguredNodeLabels().getLabelsByQueue(getQueuePath());
    } else {
      // Fallback to suboptimal but correct logic
      this.configuredNodeLabels = csContext.getConfiguration()
          .getConfiguredNodeLabels(queuePath);
    }

    // Validate the initialized settings
    validateNodeLabels();
  }

  private void validateNodeLabels() throws IOException {
    // Check if labels of this queue is a subset of parent queue, only do this
    // when the queue in question is not root
    if (parent != null && parent.getParent() != null) {
      if (parent.getAccessibleNodeLabels() != null && !parent
          .getAccessibleNodeLabels().contains(RMNodeLabelsManager.ANY)) {
        // if parent isn't "*", child shouldn't be "*" too
        if (this.getAccessibleNodeLabels().contains(
            RMNodeLabelsManager.ANY)) {
          throw new IOException("Parent's accessible queue is not ANY(*), "
              + "but child's accessible queue is *");
        } else{
          Set<String> diff = Sets.difference(this.getAccessibleNodeLabels(),
              parent.getAccessibleNodeLabels());
          if (!diff.isEmpty()) {
            throw new IOException(
                "Some labels of child queue is not a subset "
                    + "of parent queue, these labels=[" + StringUtils
                    .join(diff, ",") + "]");
          }
        }
      }
    }
  }

  private void setupApplicationLimits(CapacitySchedulerConfiguration configuration) {
    // Store max parallel apps property
    this.maxParallelApps = configuration.getMaxParallelAppsForQueue(getQueuePath());

    maxApplicationLifetime = getInheritedMaxAppLifetime(this, configuration);
    defaultApplicationLifetime =
        getInheritedDefaultAppLifetime(this, configuration,
            maxApplicationLifetime);
  }

  private void setupMaximumAllocation(CapacitySchedulerConfiguration csConf) {
    String myQueuePath = getQueuePath();
    /* YARN-10869: When using AutoCreatedLeafQueues, the passed configuration
    * object is a cloned one containing only the template configs
    * (see ManagedParentQueue#getLeafQueueConfigs). To ensure that the actual
    * cluster maximum allocation is fetched the original config object should
    * be used.
    */
    Resource clusterMax = ResourceUtils
        .fetchMaximumAllocationFromConfig(this.csContext.getConfiguration());
    Resource queueMax = csConf.getQueueMaximumAllocation(myQueuePath);

    maximumAllocation = Resources.clone(
        parent == null ? clusterMax : parent.getMaximumAllocation());

    String errMsg =
        "Queue maximum allocation cannot be larger than the cluster setting"
            + " for queue " + myQueuePath
            + " max allocation per queue: %s"
            + " cluster setting: " + clusterMax;

    if (queueMax == Resources.none()) {
      // Handle backward compatibility
      long queueMemory = csConf.getQueueMaximumAllocationMb(myQueuePath);
      int queueVcores = csConf.getQueueMaximumAllocationVcores(myQueuePath);
      if (queueMemory != UNDEFINED) {
        maximumAllocation.setMemorySize(queueMemory);
      }

      if (queueVcores != UNDEFINED) {
        maximumAllocation.setVirtualCores(queueVcores);
      }

      if ((queueMemory != UNDEFINED && queueMemory > clusterMax.getMemorySize()
          || (queueVcores != UNDEFINED
          && queueVcores > clusterMax.getVirtualCores()))) {
        throw new IllegalArgumentException(
            String.format(errMsg, maximumAllocation));
      }
    } else {
      // Queue level maximum-allocation can't be larger than cluster setting
      for (ResourceInformation ri : queueMax.getResources()) {
        if (ri.compareTo(clusterMax.getResourceInformation(ri.getName())) > 0) {
          throw new IllegalArgumentException(String.format(errMsg, queueMax));
        }

        maximumAllocation.setResourceInformation(ri.getName(), ri);
      }
    }
  }

  private Map<String, Float> getUserWeightsFromHierarchy
      (CapacitySchedulerConfiguration configuration) throws
      IOException {
    Map<String, Float> unionInheritedWeights = new HashMap<String, Float>();
    CSQueue parentQ = getParent();
    if (parentQ != null) {
      // Inherit all of parent's user's weights
      unionInheritedWeights.putAll(parentQ.getUserWeights());
    }
    // Insert this queue's user's weights, overriding parent's user's weights if
    // there is overlap.
    unionInheritedWeights.putAll(
        configuration.getAllUserWeightsForQueue(getQueuePath()));
    return unionInheritedWeights;
  }

  protected Resource getMinimumAbsoluteResource(String queuePath, String label) {
    Resource minResource = csContext.getConfiguration()
        .getMinimumResourceRequirement(label, queuePath, resourceTypes);
    return minResource;
  }

  protected Resource getMaximumAbsoluteResource(String queuePath, String label) {
    Resource maxResource = csContext.getConfiguration()
        .getMaximumResourceRequirement(label, queuePath, resourceTypes);
    return maxResource;
  }

  protected boolean checkConfigTypeIsAbsoluteResource(String queuePath,
      String label) {
    return csContext.getConfiguration().checkConfigTypeIsAbsoluteResource(label,
        queuePath, resourceTypes);
  }

  protected void updateCapacityConfigType() {
    this.capacityConfigType = CapacityConfigType.NONE;
    for (String label : configuredNodeLabels) {
      LOG.debug("capacityConfigType is '{}' for queue {}",
          capacityConfigType, getQueuePath());

      CapacityConfigType localType = checkConfigTypeIsAbsoluteResource(
          queuePath, label) ? CapacityConfigType.ABSOLUTE_RESOURCE
          : CapacityConfigType.PERCENTAGE;

      if (this.capacityConfigType.equals(CapacityConfigType.NONE)) {
        this.capacityConfigType = localType;
        LOG.debug("capacityConfigType is updated as '{}' for queue {}",
            capacityConfigType, getQueuePath());
      } else {
        validateAbsoluteVsPercentageCapacityConfig(localType);
      }
    }
  }

  protected void updateConfigurableResourceLimits(Resource clusterResource) {
    for (String label : configuredNodeLabels) {
      final Resource minResource = getMinimumAbsoluteResource(queuePath, label);
      Resource maxResource = getMaximumAbsoluteResource(queuePath, label);

      if (parent != null) {
        final Resource parentMax = parent.getQueueResourceQuotas().getConfiguredMaxResource(label);
        validateMinResourceIsNotGreaterThanMaxResource(maxResource, parentMax, clusterResource,
            "Max resource configuration "
                + maxResource + " is greater than parents max value:"
                + parentMax + " in queue:" + getQueuePath());

        // If child's max resource is not set, but its parent max resource is
        // set, we must set child max resource to its parent's.
        if (maxResource.equals(Resources.none()) &&
            !minResource.equals(Resources.none()) &&
            !parentMax.equals(Resources.none())) {
          maxResource = Resources.clone(parentMax);
        }
      }

      validateMinResourceIsNotGreaterThanMaxResource(minResource, maxResource, clusterResource,
          "Min resource configuration "
              + minResource + " is greater than its max value:" + maxResource
              + " in queue:" + getQueuePath());

      LOG.debug("Updating absolute resource configuration for queue:{} as"
              + " minResource={} and maxResource={}", getQueuePath(), minResource,
          maxResource);

      queueResourceQuotas.setConfiguredMinResource(label, minResource);
      queueResourceQuotas.setConfiguredMaxResource(label, maxResource);
    }
  }

  private void validateMinResourceIsNotGreaterThanMaxResource(Resource minResource,
                                                              Resource maxResource,
                                                              Resource clusterResource,
                                                              String validationError) {
    if (!maxResource.equals(Resources.none()) && Resources.greaterThan(
        resourceCalculator, clusterResource, minResource, maxResource)) {
      throw new IllegalArgumentException(validationError);
    }
  }

  private void validateAbsoluteVsPercentageCapacityConfig(
      CapacityConfigType localType) {
    if (!queuePath.equals("root")
        && !this.capacityConfigType.equals(localType)) {
      throw new IllegalArgumentException("Queue '" + getQueuePath()
          + "' should use either percentage based capacity"
          + " configuration or absolute resource.");
    }
  }

  @Override
  public CapacityConfigType getCapacityConfigType() {
    return capacityConfigType;
  }

  @Override
  public Resource getEffectiveCapacity(String label) {
    return Resources
        .clone(getQueueResourceQuotas().getEffectiveMinResource(label));
  }

  @Override
  public Resource getEffectiveCapacityDown(String label, Resource factor) {
    return Resources.normalizeDown(resourceCalculator,
        getQueueResourceQuotas().getEffectiveMinResource(label),
        minimumAllocation);
  }

  @Override
  public Resource getEffectiveMaxCapacity(String label) {
    return Resources
        .clone(getQueueResourceQuotas().getEffectiveMaxResource(label));
  }

  @Override
  public Resource getEffectiveMaxCapacityDown(String label, Resource factor) {
    return Resources.normalizeDown(resourceCalculator,
        getQueueResourceQuotas().getEffectiveMaxResource(label),
        minimumAllocation);
  }

  private void initializeQueueState(CapacitySchedulerConfiguration configuration) {
    QueueState previousState = getState();
    QueueState configuredState = configuration
        .getConfiguredState(getQueuePath());
    QueueState parentState = (parent == null) ? null : parent.getState();

    // verify that we can not any value for State other than RUNNING/STOPPED
    if (configuredState != null && configuredState != QueueState.RUNNING
        && configuredState != QueueState.STOPPED) {
      throw new IllegalArgumentException("Invalid queue state configuration."
          + " We can only use RUNNING or STOPPED.");
    }
    // If we did not set state in configuration, use Running as default state
    QueueState defaultState = QueueState.RUNNING;

    if (previousState == null) {
      // If current state of the queue is null, we would inherit the state
      // from its parent. If this queue does not has parent, such as root queue,
      // we would use the configured state.
      if (parentState == null) {
        updateQueueState((configuredState == null) ? defaultState
            : configuredState);
      } else {
        if (configuredState == null) {
          updateQueueState((parentState == QueueState.DRAINING) ?
              QueueState.STOPPED : parentState);
        } else if (configuredState == QueueState.RUNNING
            && parentState != QueueState.RUNNING) {
          throw new IllegalArgumentException(
              "The parent queue:" + parent.getQueuePath()
                  + " cannot be STOPPED as the child queue:" + queuePath
                  + " is in RUNNING state.");
        } else {
          updateQueueState(configuredState);
        }
      }
    } else {
      // when we get a refreshQueue request from AdminService,
      if (previousState == QueueState.RUNNING) {
        if (configuredState == QueueState.STOPPED) {
          stopQueue();
        }
      } else {
        if (configuredState == QueueState.RUNNING) {
          try {
            activeQueue();
          } catch (YarnException ex) {
            throw new IllegalArgumentException(ex.getMessage());
          }
        }
      }
    }
  }

  protected QueueInfo getQueueInfo() {
    // Deliberately doesn't use lock here, because this method will be invoked
    // from schedulerApplicationAttempt, to avoid deadlock, sacrifice
    // consistency here.
    // TODO, improve this
    QueueInfo queueInfo = recordFactory.newRecordInstance(QueueInfo.class);
    queueInfo.setQueueName(queueName);
    queueInfo.setQueuePath(queuePath);
    queueInfo.setAccessibleNodeLabels(accessibleLabels);
    queueInfo.setCapacity(queueCapacities.getCapacity());
    queueInfo.setMaximumCapacity(queueCapacities.getMaximumCapacity());
    queueInfo.setQueueState(getState());
    queueInfo.setDefaultNodeLabelExpression(defaultLabelExpression);
    queueInfo.setCurrentCapacity(getUsedCapacity());
    queueInfo.setQueueStatistics(getQueueStatistics());
    queueInfo.setPreemptionDisabled(preemptionDisabled);
    queueInfo.setIntraQueuePreemptionDisabled(
        getIntraQueuePreemptionDisabled());
    queueInfo.setQueueConfigurations(getQueueConfigurations());
    queueInfo.setWeight(queueCapacities.getWeight());
    queueInfo.setMaxParallelApps(maxParallelApps);
    return queueInfo;
  }

  public QueueStatistics getQueueStatistics() {
    // Deliberately doesn't use lock here, because this method will be invoked
    // from schedulerApplicationAttempt, to avoid deadlock, sacrifice
    // consistency here.
    // TODO, improve this
    QueueStatistics stats = recordFactory.newRecordInstance(
        QueueStatistics.class);
    stats.setNumAppsSubmitted(getMetrics().getAppsSubmitted());
    stats.setNumAppsRunning(getMetrics().getAppsRunning());
    stats.setNumAppsPending(getMetrics().getAppsPending());
    stats.setNumAppsCompleted(getMetrics().getAppsCompleted());
    stats.setNumAppsKilled(getMetrics().getAppsKilled());
    stats.setNumAppsFailed(getMetrics().getAppsFailed());
    stats.setNumActiveUsers(getMetrics().getActiveUsers());
    stats.setAvailableMemoryMB(getMetrics().getAvailableMB());
    stats.setAllocatedMemoryMB(getMetrics().getAllocatedMB());
    stats.setPendingMemoryMB(getMetrics().getPendingMB());
    stats.setReservedMemoryMB(getMetrics().getReservedMB());
    stats.setAvailableVCores(getMetrics().getAvailableVirtualCores());
    stats.setAllocatedVCores(getMetrics().getAllocatedVirtualCores());
    stats.setPendingVCores(getMetrics().getPendingVirtualCores());
    stats.setReservedVCores(getMetrics().getReservedVirtualCores());
    stats.setPendingContainers(getMetrics().getPendingContainers());
    stats.setAllocatedContainers(getMetrics().getAllocatedContainers());
    stats.setReservedContainers(getMetrics().getReservedContainers());
    return stats;
  }

  public Map<String, QueueConfigurations> getQueueConfigurations() {
    Map<String, QueueConfigurations> queueConfigurations = new HashMap<>();
    Set<String> nodeLabels = getNodeLabelsForQueue();
    for (String nodeLabel : nodeLabels) {
      QueueConfigurations queueConfiguration =
          recordFactory.newRecordInstance(QueueConfigurations.class);
      float capacity = queueCapacities.getCapacity(nodeLabel);
      float absoluteCapacity = queueCapacities.getAbsoluteCapacity(nodeLabel);
      float maxCapacity = queueCapacities.getMaximumCapacity(nodeLabel);
      float absMaxCapacity =
          queueCapacities.getAbsoluteMaximumCapacity(nodeLabel);
      float maxAMPercentage =
          queueCapacities.getMaxAMResourcePercentage(nodeLabel);
      queueConfiguration.setCapacity(capacity);
      queueConfiguration.setAbsoluteCapacity(absoluteCapacity);
      queueConfiguration.setMaxCapacity(maxCapacity);
      queueConfiguration.setAbsoluteMaxCapacity(absMaxCapacity);
      queueConfiguration.setMaxAMPercentage(maxAMPercentage);
      queueConfiguration.setConfiguredMinCapacity(
          queueResourceQuotas.getConfiguredMinResource(nodeLabel));
      queueConfiguration.setConfiguredMaxCapacity(
          queueResourceQuotas.getConfiguredMaxResource(nodeLabel));
      queueConfiguration.setEffectiveMinCapacity(
          queueResourceQuotas.getEffectiveMinResource(nodeLabel));
      queueConfiguration.setEffectiveMaxCapacity(
          queueResourceQuotas.getEffectiveMaxResource(nodeLabel));
      queueConfigurations.put(nodeLabel, queueConfiguration);
    }
    return queueConfigurations;
  }

  @Private
  public Resource getMaximumAllocation() {
    return maximumAllocation;
  }

  @Private
  public Resource getMinimumAllocation() {
    return minimumAllocation;
  }

  void allocateResource(Resource clusterResource,
      Resource resource, String nodePartition) {
    writeLock.lock();
    try {
      queueUsage.incUsed(nodePartition, resource);

      ++numContainers;

      CSQueueUtils.updateQueueStatistics(resourceCalculator, clusterResource,
          this, labelManager, nodePartition);
    } finally {
      writeLock.unlock();
    }
  }

  protected void releaseResource(Resource clusterResource,
      Resource resource, String nodePartition) {
    writeLock.lock();
    try {
      queueUsage.decUsed(nodePartition, resource);

      CSQueueUtils.updateQueueStatistics(resourceCalculator, clusterResource,
          this, labelManager, nodePartition);

      --numContainers;
    } finally {
      writeLock.unlock();
    }
  }

  @Private
  public boolean getReservationContinueLooking() {
    return reservationsContinueLooking;
  }

  @Private
  public Map<AccessType, AccessControlList> getACLs() {
    readLock.lock();
    try {
      return acls;
    } finally {
      readLock.unlock();
    }
  }

  @Private
  public boolean getPreemptionDisabled() {
    return preemptionDisabled;
  }

  @Private
  public boolean getIntraQueuePreemptionDisabled() {
    return intraQueuePreemptionDisabledInHierarchy || preemptionDisabled;
  }

  @Private
  public boolean getIntraQueuePreemptionDisabledInHierarchy() {
    return intraQueuePreemptionDisabledInHierarchy;
  }

  @Private
  public QueueCapacities getQueueCapacities() {
    return queueCapacities;
  }

  @Private
  public ResourceUsage getQueueResourceUsage() {
    return queueUsage;
  }

  @Override
  public QueueResourceQuotas getQueueResourceQuotas() {
    return queueResourceQuotas;
  }

  @Override
  public ReentrantReadWriteLock.ReadLock getReadLock() {
    return readLock;
  }

  /**
   * The specified queue is cross-queue preemptable if system-wide cross-queue
   * preemption is turned on unless any queue in the <em>qPath</em> hierarchy
   * has explicitly turned cross-queue preemption off.
   * NOTE: Cross-queue preemptability is inherited from a queue's parent.
   *
   * @param q queue to check preemption state
   * @param configuration capacity scheduler config
   * @return true if queue has cross-queue preemption disabled, false otherwise
   */
  private boolean isQueueHierarchyPreemptionDisabled(CSQueue q,
      CapacitySchedulerConfiguration configuration) {
    boolean systemWidePreemption =
        csContext.getConfiguration()
            .getBoolean(YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS,
                YarnConfiguration.DEFAULT_RM_SCHEDULER_ENABLE_MONITORS);
    CSQueue parentQ = q.getParent();

    // If the system-wide preemption switch is turned off, all of the queues in
    // the qPath hierarchy have preemption disabled, so return true.
    if (!systemWidePreemption) return true;

    // If q is the root queue and the system-wide preemption switch is turned
    // on, then q does not have preemption disabled (default=false, below)
    // unless the preemption_disabled property is explicitly set.
    if (parentQ == null) {
      return configuration.getPreemptionDisabled(q.getQueuePath(), false);
    }

    // If this is not the root queue, inherit the default value for the
    // preemption_disabled property from the parent. Preemptability will be
    // inherited from the parent's hierarchy unless explicitly overridden at
    // this level.
    return configuration.getPreemptionDisabled(q.getQueuePath(),
        parentQ.getPreemptionDisabled());
  }

  private long getInheritedMaxAppLifetime(CSQueue q,
      CapacitySchedulerConfiguration conf) {
    CSQueue parentQ = q.getParent();
    long maxAppLifetime = conf.getMaximumLifetimePerQueue(q.getQueuePath());

    // If q is the root queue, then get max app lifetime from conf.
    if (parentQ == null) {
      return maxAppLifetime;
    }

    // If this is not the root queue, get this queue's max app lifetime
    // from the conf. The parent's max app lifetime will be used if it's
    // not set for this queue.
    // A value of 0 will override the parent's value and means no max lifetime.
    // A negative value means that the parent's max should be used.
    long parentsMaxAppLifetime = getParent().getMaximumApplicationLifetime();
    return (maxAppLifetime >= 0) ? maxAppLifetime : parentsMaxAppLifetime;
  }

  private long getInheritedDefaultAppLifetime(CSQueue q,
      CapacitySchedulerConfiguration conf, long myMaxAppLifetime) {
    CSQueue parentQ = q.getParent();
    long defaultAppLifetime = conf.getDefaultLifetimePerQueue(getQueuePath());
    defaultAppLifetimeWasSpecifiedInConfig =
        (defaultAppLifetime >= 0
            || (parentQ != null &&
            parentQ.getDefaultAppLifetimeWasSpecifiedInConfig()));

    // If q is the root queue, then get default app lifetime from conf.
    if (parentQ == null) {
      return defaultAppLifetime;
    }

    // If this is not the root queue, get the parent's default app lifetime. The
    // parent's default app lifetime will be used if not set for this queue.
    long parentsDefaultAppLifetime =
        getParent().getDefaultApplicationLifetime();

    // Negative value indicates default lifetime was not set at this level.
    // If default lifetime was not set at this level, calculate it based on
    // parent's default lifetime or current queue's max lifetime.
    if (defaultAppLifetime < 0) {
      // If default lifetime was not set at this level but was set somewhere in
      // the parent's hierarchy, set default lifetime to parent queue's default
      // only if parent queue's lifetime is less than current queue's max
      // lifetime. Otherwise, use current queue's max lifetime value for its
      // default lifetime.
      if (defaultAppLifetimeWasSpecifiedInConfig) {
        defaultAppLifetime =
            Math.min(parentsDefaultAppLifetime, myMaxAppLifetime);
      } else {
        // Default app lifetime value was not set anywhere in this queue's
        // hierarchy. Use current queue's max lifetime as its default.
        defaultAppLifetime = myMaxAppLifetime;
      }
    } // else if >= 0, default lifetime was set at this level. Just use it.

    if (myMaxAppLifetime > 0 &&
        defaultAppLifetime > myMaxAppLifetime) {
      throw new YarnRuntimeException(
          "Default lifetime " + defaultAppLifetime
              + " can't exceed maximum lifetime " + myMaxAppLifetime);
    }
    
    if (defaultAppLifetime <= 0) {
      defaultAppLifetime = myMaxAppLifetime;
    }
    return defaultAppLifetime;
  }

  /**
   * The specified queue is intra-queue preemptable if
   * 1) system-wide intra-queue preemption is turned on
   * 2) no queue in the <em>qPath</em> hierarchy has explicitly turned off intra
   *    queue preemption.
   * NOTE: Intra-queue preemptability is inherited from a queue's parent.
   *
   * @param q queue to check intra-queue preemption state
   * @param configuration capacity scheduler config
   * @return true if queue has intra-queue preemption disabled, false otherwise
   */
  private boolean isIntraQueueHierarchyPreemptionDisabled(CSQueue q,
      CapacitySchedulerConfiguration configuration) {
    boolean systemWideIntraQueuePreemption =
        csContext.getConfiguration().getBoolean(
            CapacitySchedulerConfiguration.INTRAQUEUE_PREEMPTION_ENABLED,
            CapacitySchedulerConfiguration
                .DEFAULT_INTRAQUEUE_PREEMPTION_ENABLED);
    // Intra-queue preemption is disabled for this queue if the system-wide
    // intra-queue preemption flag is false
    if (!systemWideIntraQueuePreemption) return true;

    // Check if this is the root queue and the root queue's intra-queue
    // preemption disable switch is set
    CSQueue parentQ = q.getParent();
    if (parentQ == null) {
      return configuration
          .getIntraQueuePreemptionDisabled(q.getQueuePath(), false);
    }

    // At this point, the master preemption switch is enabled down to this
    // queue's level. Determine whether or not intra-queue preemption is enabled
    // down to this queue's level and return that value.
    return configuration.getIntraQueuePreemptionDisabled(q.getQueuePath(),
        parentQ.getIntraQueuePreemptionDisabledInHierarchy());
  }

  private Resource getCurrentLimitResource(String nodePartition,
      Resource clusterResource, ResourceLimits currentResourceLimits,
      SchedulingMode schedulingMode) {
    if (schedulingMode == SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY) {
      /*
       * Current limit resource: For labeled resource: limit = queue-max-resource
       * (TODO, this part need update when we support labeled-limit) For
       * non-labeled resource: limit = min(queue-max-resource,
       * limit-set-by-parent)
       */
      Resource queueMaxResource =
          getQueueMaxResource(nodePartition);

      return Resources.min(resourceCalculator, clusterResource,
          queueMaxResource, currentResourceLimits.getLimit());
    } else if (schedulingMode == SchedulingMode.IGNORE_PARTITION_EXCLUSIVITY) {
      // When we doing non-exclusive resource allocation, maximum capacity of
      // all queues on this label equals to total resource with the label.
      return labelManager.getResourceByLabel(nodePartition, clusterResource);
    }

    return Resources.none();
  }

  Resource getQueueMaxResource(String nodePartition) {
    return getEffectiveMaxCapacity(nodePartition);
  }

  public boolean hasChildQueues() {
    List<CSQueue> childQueues = getChildQueues();
    return childQueues != null && !childQueues.isEmpty();
  }

  boolean canAssignToThisQueue(Resource clusterResource,
      String nodePartition, ResourceLimits currentResourceLimits,
      Resource resourceCouldBeUnreserved, SchedulingMode schedulingMode) {
    readLock.lock();
    try {
      // Get current limited resource:
      // - When doing RESPECT_PARTITION_EXCLUSIVITY allocation, we will respect
      // queues' max capacity.
      // - When doing IGNORE_PARTITION_EXCLUSIVITY allocation, we will not respect
      // queue's max capacity, queue's max capacity on the partition will be
      // considered to be 100%. Which is a queue can use all resource in the
      // partition.
      // Doing this because: for non-exclusive allocation, we make sure there's
      // idle resource on the partition, to avoid wastage, such resource will be
      // leveraged as much as we can, and preemption policy will reclaim it back
      // when partitioned-resource-request comes back.
      Resource currentLimitResource = getCurrentLimitResource(nodePartition,
          clusterResource, currentResourceLimits, schedulingMode);

      Resource nowTotalUsed = queueUsage.getUsed(nodePartition);

      // Set headroom for currentResourceLimits:
      // When queue is a parent queue: Headroom = limit - used + killable
      // When queue is a leaf queue: Headroom = limit - used (leaf queue cannot preempt itself)
      Resource usedExceptKillable = nowTotalUsed;
      if (hasChildQueues()) {
        usedExceptKillable = Resources.subtract(nowTotalUsed,
            getTotalKillableResource(nodePartition));
      }
      currentResourceLimits.setHeadroom(
          Resources.subtract(currentLimitResource, usedExceptKillable));

      if (Resources.greaterThanOrEqual(resourceCalculator, clusterResource,
          usedExceptKillable, currentLimitResource)) {

        // if reservation continue looking enabled, check to see if could we
        // potentially use this node instead of a reserved node if the application
        // has reserved containers.
        if (this.reservationsContinueLooking
            && Resources.greaterThan(resourceCalculator, clusterResource,
            resourceCouldBeUnreserved, Resources.none())) {
          // resource-without-reserved = used - reserved
          Resource newTotalWithoutReservedResource = Resources.subtract(
              usedExceptKillable, resourceCouldBeUnreserved);

          // when total-used-without-reserved-resource < currentLimit, we still
          // have chance to allocate on this node by unreserving some containers
          if (Resources.lessThan(resourceCalculator, clusterResource,
              newTotalWithoutReservedResource, currentLimitResource)) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("try to use reserved: " + getQueuePath()
                  + " usedResources: " + queueUsage.getUsed()
                  + ", clusterResources: " + clusterResource
                  + ", reservedResources: " + resourceCouldBeUnreserved
                  + ", capacity-without-reserved: "
                  + newTotalWithoutReservedResource
                  + ", maxLimitCapacity: " + currentLimitResource);
            }
            return true;
          }
        }

        // Can not assign to this queue
        if (LOG.isDebugEnabled()) {
          LOG.debug("Failed to assign to queue: " + getQueuePath()
              + " nodePartition: " + nodePartition
              + ", usedResources: " + queueUsage.getUsed(nodePartition)
              + ", clusterResources: " + clusterResource
              + ", reservedResources: " + resourceCouldBeUnreserved
              + ", maxLimitCapacity: " + currentLimitResource
              + ", currTotalUsed:" + usedExceptKillable);
        }
        return false;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Check assign to queue: " + getQueuePath()
            + " nodePartition: " + nodePartition
            + ", usedResources: " + queueUsage.getUsed(nodePartition)
            + ", clusterResources: " + clusterResource
            + ", currentUsedCapacity: " + Resources
            .divide(resourceCalculator, clusterResource,
                queueUsage.getUsed(nodePartition), labelManager
                    .getResourceByLabel(nodePartition, clusterResource))
            + ", max-capacity: " + queueCapacities
            .getAbsoluteMaximumCapacity(nodePartition));
      }
      return true;
    } finally {
      readLock.unlock();
    }

  }

  private static String ensurePartition(String partition) {
    return Optional.ofNullable(partition).orElse(RMNodeLabelsManager.NO_LABEL);
  }

  @FunctionalInterface
  interface Counter {
    void count(String partition, Resource resource);
  }

  @FunctionalInterface
  interface CounterWithApp {
    void count(String partition, Resource reservedRes, SchedulerApplicationAttempt application);
  }

  private void count(String partition, Resource resource, Counter counter, Counter parentCounter) {
    final String checkedPartition = ensurePartition(partition);
    counter.count(checkedPartition, resource);
    Optional.ofNullable(parentCounter).ifPresent(c -> c.count(checkedPartition, resource));
  }

  private void countAndUpdate(String partition, Resource resource,
                              Counter counter, CounterWithApp parentCounter) {
    final String checkedPartition = ensurePartition(partition);
    counter.count(checkedPartition, resource);
    CSQueueUtils.updateUsedCapacity(resourceCalculator,
        labelManager.getResourceByLabel(checkedPartition, Resources.none()),
        checkedPartition, this);
    Optional.ofNullable(parentCounter).ifPresent(c -> c.count(checkedPartition, resource, null));
  }

  @Override
  public void incReservedResource(String partition, Resource reservedRes) {
    count(partition, reservedRes, queueUsage::incReserved,
        parent == null ? null : parent::incReservedResource);
  }

  @Override
  public void decReservedResource(String partition, Resource reservedRes) {
    count(partition, reservedRes, queueUsage::decReserved,
        parent == null ? null : parent::decReservedResource);
  }

  @Override
  public void incPendingResource(String nodeLabel, Resource resourceToInc) {
    count(nodeLabel, resourceToInc, queueUsage::incPending,
        parent == null ? null : parent::incPendingResource);
  }

  @Override
  public void decPendingResource(String nodeLabel, Resource resourceToDec) {
    count(nodeLabel, resourceToDec, queueUsage::decPending,
        parent == null ? null : parent::decPendingResource);
  }

  @Override
  public void incUsedResource(String nodeLabel, Resource resourceToInc,
      SchedulerApplicationAttempt application) {
    countAndUpdate(nodeLabel, resourceToInc, queueUsage::incUsed,
        parent == null ? null : parent::incUsedResource);
  }

  @Override
  public void decUsedResource(String nodeLabel, Resource resourceToDec,
      SchedulerApplicationAttempt application) {
    countAndUpdate(nodeLabel, resourceToDec, queueUsage::decUsed,
        parent == null ? null : parent::decUsedResource);
  }

  /**
   * Return if the queue has pending resource on given nodePartition and
   * schedulingMode.
   */
  boolean hasPendingResourceRequest(String nodePartition,
      Resource cluster, SchedulingMode schedulingMode) {
    return SchedulerUtils.hasPendingResourceRequest(resourceCalculator,
        queueUsage, nodePartition, cluster, schedulingMode);
  }

  public boolean accessibleToPartition(String nodePartition) {
    // if queue's label is *, it can access any node
    if (accessibleLabels != null
        && accessibleLabels.contains(RMNodeLabelsManager.ANY)) {
      return true;
    }
    // any queue can access to a node without label
    if (nodePartition == null
        || nodePartition.equals(RMNodeLabelsManager.NO_LABEL)) {
      return true;
    }
    // a queue can access to a node only if it contains any label of the node
    if (accessibleLabels != null && accessibleLabels.contains(nodePartition)) {
      return true;
    }
    // sorry, you cannot access
    return false;
  }

  @Override
  public Priority getDefaultApplicationPriority() {
    // TODO add dummy implementation
    return null;
  }

  @Override
  public Set<String> getNodeLabelsForQueue() {
    // if queue's label is *, queue can access any labels. Instead of
    // considering all labels in cluster, only those labels which are
    // use some resource of this queue can be considered.
    Set<String> nodeLabels = new HashSet<String>();
    if (this.getAccessibleNodeLabels() != null && this.getAccessibleNodeLabels()
        .contains(RMNodeLabelsManager.ANY)) {
      nodeLabels.addAll(Sets.union(this.getQueueCapacities().getNodePartitionsSet(),
          this.getQueueResourceUsage().getNodePartitionsSet()));
    } else {
      nodeLabels.addAll(this.getAccessibleNodeLabels());
    }

    // Add NO_LABEL also to this list as NO_LABEL also can be granted with
    // resource in many general cases.
    if (!nodeLabels.contains(RMNodeLabelsManager.NO_LABEL)) {
      nodeLabels.add(RMNodeLabelsManager.NO_LABEL);
    }
    return nodeLabels;
  }

  public Resource getTotalKillableResource(String partition) {
    return csContext.getPreemptionManager().getKillableResource(getQueuePath(),
        partition);
  }

  public Iterator<RMContainer> getKillableContainers(String partition) {
    return csContext.getPreemptionManager().getKillableContainers(
        getQueuePath(),
        partition);
  }

  @VisibleForTesting
  @Override
  public CSAssignment assignContainers(Resource clusterResource,
      FiCaSchedulerNode node, ResourceLimits resourceLimits,
      SchedulingMode schedulingMode) {
    return assignContainers(clusterResource, new SimpleCandidateNodeSet<>(node),
        resourceLimits, schedulingMode);
  }

  @Override
  public boolean accept(Resource cluster,
      ResourceCommitRequest<FiCaSchedulerApp, FiCaSchedulerNode> request) {
    // Do we need to check parent queue before making this decision?
    boolean checkParentQueue = false;

    ContainerAllocationProposal<FiCaSchedulerApp, FiCaSchedulerNode> allocation =
        request.getFirstAllocatedOrReservedContainer();
    SchedulerContainer<FiCaSchedulerApp, FiCaSchedulerNode> schedulerContainer =
        allocation.getAllocatedOrReservedContainer();

    // Do not check when allocating new container from a reserved container
    if (allocation.getAllocateFromReservedContainer() == null) {
      Resource required = allocation.getAllocatedOrReservedResource();
      Resource netAllocated = Resources.subtract(required,
          request.getTotalReleasedResource());

      readLock.lock();
      try {
        String partition = schedulerContainer.getNodePartition();
        Resource maxResourceLimit;
        if (allocation.getSchedulingMode()
            == SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY) {
          maxResourceLimit = getQueueMaxResource(partition);
        } else{
          maxResourceLimit = labelManager.getResourceByLabel(
              schedulerContainer.getNodePartition(), cluster);
        }
        if (!Resources.fitsIn(resourceCalculator,
            Resources.add(queueUsage.getUsed(partition), netAllocated),
            maxResourceLimit)) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Used resource=" + queueUsage.getUsed(partition)
                + " exceeded maxResourceLimit of the queue ="
                + maxResourceLimit);
          }
          return false;
        }
      } finally {
        readLock.unlock();
      }

      // Only check parent queue when something new allocated or reserved.
      checkParentQueue = true;
    }


    if (parent != null && checkParentQueue) {
      return parent.accept(cluster, request);
    }

    return true;
  }

  @Override
  public void validateSubmitApplication(ApplicationId applicationId,
      String userName, String queue) throws AccessControlException {
    // Dummy implementation
  }

  @Override
  public void updateQueueState(QueueState queueState) {
    this.state = queueState;
  }

  @Override
  public void activeQueue() throws YarnException {
    this.writeLock.lock();
    try {
      if (getState() == QueueState.RUNNING) {
        LOG.info("The specified queue:" + getQueuePath()
            + " is already in the RUNNING state.");
      } else {
        CSQueue parent = getParent();
        if (parent == null || parent.getState() == QueueState.RUNNING) {
          updateQueueState(QueueState.RUNNING);
        } else {
          throw new YarnException("The parent Queue:" + parent.getQueuePath()
              + " is not running. Please activate the parent queue first");
        }
      }
    } finally {
      this.writeLock.unlock();
    }
  }

  protected void appFinished() {
    this.writeLock.lock();
    try {
      if (getState() == QueueState.DRAINING) {
        if (getNumApplications() == 0) {
          updateQueueState(QueueState.STOPPED);
        }
      }
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public Priority getPriority() {
    return this.priority;
  }

  @Override
  public Map<String, Float> getUserWeights() {
    return userWeights;
  }

  public void recoverDrainingState() {
    this.writeLock.lock();
    try {
      if (getState() == QueueState.STOPPED) {
        updateQueueState(QueueState.DRAINING);
      }
      LOG.info("Recover draining state for queue " + this.getQueuePath());
      if (getParent() != null && getParent().getState() == QueueState.STOPPED) {
        ((AbstractCSQueue) getParent()).recoverDrainingState();
      }
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public String getMultiNodeSortingPolicyName() {
    return this.multiNodeSortingPolicyName;
  }

  public void setMultiNodeSortingPolicyName(String policyName) {
    this.multiNodeSortingPolicyName = policyName;
  }

  public long getMaximumApplicationLifetime() {
    return maxApplicationLifetime;
  }

  public long getDefaultApplicationLifetime() {
    return defaultApplicationLifetime;
  }

  public boolean getDefaultAppLifetimeWasSpecifiedInConfig() {
    return defaultAppLifetimeWasSpecifiedInConfig;
  }

  public void setMaxParallelApps(int maxParallelApps) {
    this.maxParallelApps = maxParallelApps;
  }

  @Override
  public int getMaxParallelApps() {
    return maxParallelApps;
  }

  abstract int getNumRunnableApps();

  protected void updateAbsoluteCapacities() {
    QueueCapacities parentQueueCapacities = null;
    if (parent != null) {
      parentQueueCapacities = parent.getQueueCapacities();
    }

    CSQueueUtils.updateAbsoluteCapacitiesByNodeLabels(queueCapacities,
        parentQueueCapacities, queueCapacities.getExistingNodeLabels());
  }

  private Resource createNormalizedMinResource(Resource minResource,
      Map<String, Float> effectiveMinRatio) {
    Resource ret = Resource.newInstance(minResource);
    int maxLength = ResourceUtils.getNumberOfCountableResourceTypes();
    for (int i = 0; i < maxLength; i++) {
      ResourceInformation nResourceInformation =
          minResource.getResourceInformation(i);

      Float ratio = effectiveMinRatio.get(nResourceInformation.getName());
      if (ratio != null) {
        ret.setResourceValue(i,
            (long) (nResourceInformation.getValue() * ratio));
        if (LOG.isDebugEnabled()) {
          LOG.debug("Updating min resource for Queue: " + queuePath + " as " + ret
              .getResourceInformation(i) + ", Actual resource: "
              + nResourceInformation.getValue() + ", ratio: " + ratio);
        }
      }
    }
    return ret;
  }

  private Resource getOrInheritMaxResource(Resource resourceByLabel, String label) {
    Resource parentMaxResource =
        parent.getQueueResourceQuotas().getConfiguredMaxResource(label);
    if (parentMaxResource.equals(Resources.none())) {
      parentMaxResource =
          parent.getQueueResourceQuotas().getEffectiveMaxResource(label);
    }

    Resource configuredMaxResource =
        getQueueResourceQuotas().getConfiguredMaxResource(label);
    if (configuredMaxResource.equals(Resources.none())) {
      return Resources.clone(parentMaxResource);
    }

    return Resources.clone(Resources.min(resourceCalculator, resourceByLabel,
        configuredMaxResource, parentMaxResource));
  }

  void updateMaxAppRelatedField(CapacitySchedulerConfiguration conf,
      LeafQueue leafQueue) {
    int maxApplications = conf.getMaximumApplicationsPerQueue(queuePath);
    int maxGlobalPerQueueApps = conf.getGlobalMaximumApplicationsPerQueue();
    String maxLabel = RMNodeLabelsManager.NO_LABEL;

    if (maxApplications < 0) {
      for (String label : configuredNodeLabels) {
        int maxApplicationsByLabel = 0;
        if (maxGlobalPerQueueApps > 0) {
          // In absolute mode, should
          // shrink when change to corresponding label capacity.
          maxApplicationsByLabel = this.capacityConfigType
              != CapacityConfigType.ABSOLUTE_RESOURCE ?
              maxGlobalPerQueueApps :
              (int) (maxGlobalPerQueueApps * queueCapacities
                  .getAbsoluteCapacity(label));
        } else {
          maxApplicationsByLabel = (int) (conf.getMaximumSystemApplications()
              * queueCapacities.getAbsoluteCapacity(label));
        }
        if (maxApplicationsByLabel > maxApplications) {
          maxApplications = maxApplicationsByLabel;
          maxLabel = label;
        }
      }
    }
    leafQueue.setMaxApplications(maxApplications);

    int maxApplicationsPerUser = Math.min(maxApplications,
        (int) (maxApplications
            * (leafQueue.getUsersManager().getUserLimit() / 100.0f)
            * leafQueue.getUsersManager().getUserLimitFactor()));
    if (leafQueue.getUsersManager().getUserLimitFactor() == -1) {
      maxApplicationsPerUser =  maxApplications;
    }

    leafQueue.setMaxApplicationsPerUser(maxApplicationsPerUser);
    LOG.info("LeafQueue:" + leafQueue.getQueuePath() +
        "update max app related, maxApplications="
        + maxApplications + ", maxApplicationsPerUser="
        + maxApplicationsPerUser + ", Abs Cap:" + queueCapacities
        .getAbsoluteCapacity(maxLabel) + ", Cap: " + queueCapacities
        .getCapacity(maxLabel) + ", MaxCap : " + queueCapacities
        .getMaximumCapacity(maxLabel));
  }

  void deriveCapacityFromAbsoluteConfigurations(String label,
      Resource clusterResource) {
    // Update capacity with a float calculated from the parent's minResources
    // and the recently changed queue minResources.
    // capacity = effectiveMinResource / {parent's effectiveMinResource}
    float result = resourceCalculator.divide(clusterResource,
        queueResourceQuotas.getEffectiveMinResource(label),
        parent.getQueueResourceQuotas().getEffectiveMinResource(label));
    queueCapacities.setCapacity(label,
        Float.isInfinite(result) ? 0 : result);

    // Update maxCapacity with a float calculated from the parent's maxResources
    // and the recently changed queue maxResources.
    // maxCapacity = effectiveMaxResource / parent's effectiveMaxResource
    result = resourceCalculator.divide(clusterResource,
        queueResourceQuotas.getEffectiveMaxResource(label),
        parent.getQueueResourceQuotas().getEffectiveMaxResource(label));
    queueCapacities.setMaximumCapacity(label,
        Float.isInfinite(result) ? 0 : result);

    // Update absolute capacity (as in fraction of the
    // whole cluster's resources) with a float calculated from the queue's
    // capacity and the parent's absoluteCapacity.
    // absoluteCapacity = capacity * parent's absoluteCapacity
    queueCapacities.setAbsoluteCapacity(label,
        queueCapacities.getCapacity(label) * parent.getQueueCapacities()
            .getAbsoluteCapacity(label));

    // Update absolute maxCapacity (as in fraction of the
    // whole cluster's resources) with a float calculated from the queue's
    // maxCapacity and the parent's absoluteMaxCapacity.
    // absoluteMaxCapacity = maxCapacity * parent's absoluteMaxCapacity
    queueCapacities.setAbsoluteMaximumCapacity(label,
        queueCapacities.getMaximumCapacity(label) *
            parent.getQueueCapacities()
                .getAbsoluteMaximumCapacity(label));
  }

  void updateEffectiveResources(Resource clusterResource) {
    for (String label : configuredNodeLabels) {
      Resource resourceByLabel = labelManager.getResourceByLabel(label,
          clusterResource);
      Resource newEffectiveMinResource;
      Resource newEffectiveMaxResource;

      // Absolute and relative/weight mode needs different handling.
      if (getCapacityConfigType().equals(
          CapacityConfigType.ABSOLUTE_RESOURCE)) {
        newEffectiveMinResource = createNormalizedMinResource(
            queueResourceQuotas.getConfiguredMinResource(label),
            ((ParentQueue) parent).getEffectiveMinRatioPerResource());

        // Max resource of a queue should be the minimum of {parent's maxResources,
        // this queue's maxResources}. Both parent's maxResources and this queue's
        // maxResources can be configured. If this queue's maxResources is not
        // configured, inherit the value from the parent. If parent's
        // maxResources is not configured its inherited value must be collected.
        newEffectiveMaxResource =
            getOrInheritMaxResource(resourceByLabel, label);
      } else {
        newEffectiveMinResource = Resources
            .multiply(resourceByLabel,
                queueCapacities.getAbsoluteCapacity(label));
        newEffectiveMaxResource = Resources
            .multiply(resourceByLabel,
                queueCapacities.getAbsoluteMaximumCapacity(label));
      }

      // Update the effective min
      queueResourceQuotas.setEffectiveMinResource(label,
          newEffectiveMinResource);
      queueResourceQuotas.setEffectiveMaxResource(label,
          newEffectiveMaxResource);

      if (LOG.isDebugEnabled()) {
        LOG.debug("Updating queue:" + queuePath
            + " with effective minimum resource=" + newEffectiveMinResource
            + "and effective maximum resource="
            + newEffectiveMaxResource);
      }

      if (getCapacityConfigType().equals(
          CapacityConfigType.ABSOLUTE_RESOURCE)) {
        /*
         * If the queues are configured with absolute resources, it is advised
         * to update capacity/max-capacity/etc. based on the newly calculated
         * resource values. These values won't be used for actual resource
         * distribution, however, for accurate metrics and the UI
         * they should be re-calculated.
         */
        deriveCapacityFromAbsoluteConfigurations(label, clusterResource);
      }
    }
  }

  public boolean isDynamicQueue() {
    readLock.lock();

    try {
      return dynamicQueue;
    } finally {
      readLock.unlock();
    }
  }

  public void setDynamicQueue(boolean dynamicQueue) {
    writeLock.lock();

    try {
      this.dynamicQueue = dynamicQueue;
    } finally {
      writeLock.unlock();
    }
  }

  protected String getCapacityOrWeightString() {
    if (queueCapacities.getWeight() != -1) {
      return "weight=" + queueCapacities.getWeight() + ", " +
          "normalizedWeight=" + queueCapacities.getNormalizedWeight();
    } else {
      return "capacity=" + queueCapacities.getCapacity();
    }
  }

  public boolean isEligibleForAutoDeletion() {
    return false;
  }

  public boolean isInactiveDynamicQueue() {
    long idleDurationSeconds =
        (Time.monotonicNow() - getLastSubmittedTimestamp())/1000;
    return isDynamicQueue() && isEligibleForAutoDeletion() &&
        (idleDurationSeconds > this.csContext.getConfiguration().
            getAutoExpiredDeletionTime());
  }

  public void updateLastSubmittedTimeStamp() {
    writeLock.lock();
    try {
      this.lastSubmittedTimestamp = Time.monotonicNow();
    } finally {
      writeLock.unlock();
    }
  }

  public long getLastSubmittedTimestamp() {
    readLock.lock();

    try {
      return lastSubmittedTimestamp;
    } finally {
      readLock.unlock();
    }
  }

  @VisibleForTesting
  public void setLastSubmittedTimestamp(long lastSubmittedTimestamp) {
    writeLock.lock();
    try {
      this.lastSubmittedTimestamp = lastSubmittedTimestamp;
    } finally {
      writeLock.unlock();
    }
  }
}
