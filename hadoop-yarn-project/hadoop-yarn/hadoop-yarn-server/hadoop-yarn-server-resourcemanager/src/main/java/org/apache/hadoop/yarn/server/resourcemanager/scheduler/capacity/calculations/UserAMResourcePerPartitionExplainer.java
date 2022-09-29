package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.calculations;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueAllocationSettings;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacities;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.UsersManager;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class UserAMResourcePerPartitionExplainer extends CSCalculationExplainer {
  private static final Logger LOG = LoggerFactory.getLogger(UserAMResourcePerPartitionExplainer.class);
  private String nodePartition;
  private String userName;
  private UsersManager.User user;
  private Resource lastClusterResource;
  //TODO add arbitrary logging prefix e.g. ENGESC-xxxx
  //TODO Configure logger by enabling calculation level logger + clas logger
  
  public UserAMResourcePerPartitionExplainer(CSCalculationType type, boolean explain) {
    super(type, explain);
  }

  public float getUserWeight() {
    float userWeight = 1.0f;
    if (userName != null && user != null) {
      userWeight = user.getWeight();
    }

    if (explain) {
      LOG.debug("User weight for user " + userName + ": " + userWeight);
    }

    return userWeight;
  }

  public float calculateEffectiveUserLimit(UsersManager usersManager) {
    float effectiveUserLimit = Math.max(usersManager.getUserLimit() / 100.0f,
        1.0f / Math.max(usersManager.getNumActiveUsers(), 1));
    
    if (explain) {
      LOG.debug("userLimit: " + usersManager.getUserLimit() +
          ", numActiveUsers: " + usersManager.getNumActiveUsers() +
          ", effectiveUserLimit: " + effectiveUserLimit);
    }
    
    return effectiveUserLimit;
  }

  public float calculateEffectiveUserLimitTimesWeight(float effectiveUserLimit, float userWeight) {
    float newEffectiveUserLimit = Math.min(effectiveUserLimit * userWeight, 1.0f);
    if (explain) {
      LOG.debug("effectiveUserLimit: " + effectiveUserLimit +
          ", userWeight: " + userWeight +
          ", newEffectiveUserLimit: " + newEffectiveUserLimit);
    }
    
    return newEffectiveUserLimit;
  }

  public void initCtx(AbstractLeafQueue abstractLeafQueue, ReentrantReadWriteLock.ReadLock readLock) {
    this.ctx = new CalculationContext(abstractLeafQueue, readLock);
  }
  
  public void initSpecific(String nodePartition, String userName, Resource lastClusterResource) {
    this.nodePartition = nodePartition;
    this.userName = userName;
    this.user = ctx.queue.getUser(userName);
    this.lastClusterResource = lastClusterResource;
  }

  public Resource calculate() {
    float userWeight = getUserWeight();

    ctx.readLock.lock();
    try {
      //Save some local variables
      final UsersManager usersManager = ctx.queue.getUsersManager();
      QueueAllocationSettings queueAllocationSettings = ctx.queue.getQueueAllocationSettings();
      QueueCapacities queueCapacities = ctx.queue.getQueueCapacities();

      float effectiveUserLimit = calculateEffectiveUserLimit(usersManager);
      float preWeightedUserLimit = effectiveUserLimit;
      log("preWeightedUserLimit", preWeightedUserLimit);
      effectiveUserLimit = calculateEffectiveUserLimitTimesWeight(effectiveUserLimit, userWeight);
      

      Resource queuePartitionResource = ctx.queue.getEffectiveCapacity(nodePartition);
      log("queuePartitionResource", "effectiveCapacity", queuePartitionResource);

      Resource minimumAllocation = queueAllocationSettings.getMinimumAllocation();
      log("minimumAllocation", minimumAllocation);

      float maxAMResourcePercentage = queueCapacities.getMaxAMResourcePercentage(nodePartition);
      log("maxAMResourcePercentage", maxAMResourcePercentage)
      Resource userAMLimit = computeUserAMLimit(effectiveUserLimit, queuePartitionResource,
          minimumAllocation, maxAMResourcePercentage);

      Resource preWeighteduserAMLimit = computeUserAMLimit(preWeightedUserLimit, queuePartitionResource,
          minimumAllocation, maxAMResourcePercentage);
      ctx.queue.getUsageTracker().getQueueUsage().setUserAMLimit(nodePartition, preWeighteduserAMLimit);

      LOG.debug("Effective user AM limit for \"{}\":{}. Effective weighted"
              + " user AM limit: {}. User weight: {}", userName,
          preWeighteduserAMLimit, userAMLimit, userWeight);
      return userAMLimit;
    } finally {
      ctx.readLock.unlock();
    }
  }

  private Resource computeUserAMLimit(float effectiveUserLimit,
      Resource queuePartitionResource, Resource minimumAllocation, float maxAMResourcePercentage) {
    final ResourceCalculator resourceCalculator = ctx.queue.getQueueContext().getResourceCalculator();
    final UsersManager usersManager = ctx.queue.getUsersManager();
    final float ulf = ctx.queue.getUserLimitFactor();
    Resource amResourceLimitPerPartition = ctx.queue.getAMResourceLimitPerPartition(nodePartition);
    
    Resource userAMLimit = Resources.multiplyAndNormalizeUp(
        resourceCalculator, queuePartitionResource,
        maxAMResourcePercentage
            * effectiveUserLimit * usersManager.getUserLimitFactor(),
        minimumAllocation);

    if (ulf == -1) {
      userAMLimit = Resources.multiplyAndNormalizeUp(
          resourceCalculator, queuePartitionResource,
          maxAMResourcePercentage,
          minimumAllocation);
    }

    userAMLimit = Resources.min(resourceCalculator, lastClusterResource,
            userAMLimit,
            Resources.clone(amResourceLimitPerPartition));
    return userAMLimit;
  }
}
