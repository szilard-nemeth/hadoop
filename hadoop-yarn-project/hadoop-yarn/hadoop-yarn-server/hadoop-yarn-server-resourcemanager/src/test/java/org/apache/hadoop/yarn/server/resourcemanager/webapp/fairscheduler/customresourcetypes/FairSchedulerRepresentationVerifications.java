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

package org.apache.hadoop.yarn.server.resourcemanager.webapp.fairscheduler.customresourcetypes;

import com.google.common.collect.Sets;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.representationhelper.ElementWrapper;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * This test helper class is primarily used by
 * {@link TestRMWebServicesFairSchedulerCustomResourceTypes}.
 */
public class FairSchedulerRepresentationVerifications {

  private static final Set<String> RESOURCE_FIELDS =
      Sets.newHashSet("minResources", "amUsedResources", "amMaxResources",
          "fairResources", "clusterResources", "reservedResources",
          "maxResources", "usedResources", "steadyFairResources",
          "demandResources");
  private final Set<String> customResourceTypes;

  public FairSchedulerRepresentationVerifications(List<String>
      customResourceTypes) {
    this.customResourceTypes = Sets.newHashSet(customResourceTypes);
  }

  public void verify(ElementWrapper wrapper) {
    verifyResourcesContainDefaultResourceTypes(wrapper, RESOURCE_FIELDS);
    verifyResourcesContainCustomResourceTypes(wrapper, RESOURCE_FIELDS);
  }

  private void verifyResourcesContainDefaultResourceTypes(ElementWrapper queue,
      Set<String> resourceCategories) {
    for (String resourceCategory : resourceCategories) {
      assertTrue("Queue " + queue + " does not have resource category field: "
          + resourceCategory, queue.hasChild(resourceCategory));
      verifyResourceContainsDefaultResourceTypes(queue.getChild(resourceCategory));
    }
  }

  private void verifyResourceContainsDefaultResourceTypes(
      ElementWrapper wrapper) {
    Object memory = wrapper.opt("memory");
    Object vCores = wrapper.opt("vCores");

    assertNotNull("Field 'memory' not found in: " + wrapper, memory);
    assertNotNull("Field 'vCores' not found in: " + wrapper, vCores);
  }

  private void verifyResourcesContainCustomResourceTypes(ElementWrapper queue,
      Set<String> resourceCategories) {
    for (String resourceCategory : resourceCategories) {
      boolean hasResourceCategory = queue.hasChild(resourceCategory);
      assertTrue("Queue " + queue + " does not have key for resourceCategory: "
          + resourceCategory, hasResourceCategory);
      for (String resourceType : customResourceTypes) {
        boolean hasResourceType = queue.hasChild(resourceType);
        assertTrue(
            "ElementWrapper " + queue
                + " does not have expected resource type: " + resourceType,
            hasResourceType);
        Long resourceTypeValue = queue.getLong(resourceType);
        assertNotNull(resourceTypeValue);
      }
    }
  }
}