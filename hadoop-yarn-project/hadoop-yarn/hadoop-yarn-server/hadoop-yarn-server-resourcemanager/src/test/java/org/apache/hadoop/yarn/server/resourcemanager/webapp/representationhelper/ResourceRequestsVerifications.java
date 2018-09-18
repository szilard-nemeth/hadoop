/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.webapp.representationhelper;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceTypes;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.junit.Assert;
import org.w3c.dom.Element;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static junit.framework.TestCase.assertTrue;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp
    .fairscheduler.customresourcetypes.CustomResourceValueExtractor
    .extractCustomResourceTypes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Performs value verifications on
 * {@link org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ResourceRequestInfo}
 * objects against the values of {@link ResourceRequest}. With the help of the
 * {@link Builder}, users can also make verifications of the custom resource
 * types and its values.
 */
public class ResourceRequestsVerifications {
  private final ResourceRequest resourceRequest;
  private final ElementWrapper requestInfo;
  private final Map<String, Long> customResourceTypes;
  private final List<String> expectedCustomResourceTypes;

  ResourceRequestsVerifications(Builder builder) {
    this.resourceRequest = builder.resourceRequest;
    this.requestInfo = builder.requestInfo;
    this.customResourceTypes = builder.customResourceTypes;
    this.expectedCustomResourceTypes = builder.expectedCustomResourceTypes;
  }

  public static void verify(ElementWrapper requestInfo, ResourceRequest rr) {
    createDefaultBuilder(requestInfo, rr)
            .build()
            .verify();
  }

  public static void verifyWithCustomResourceTypes(ElementWrapper requestInfo,
      ResourceRequest resourceRequest, List<String> expectedResourceTypes) {

    createDefaultBuilder(requestInfo, resourceRequest)
        .withExpectedCustomResourceTypes(expectedResourceTypes)
        .withCustomResourceTypes(extractActualCustomResourceTypes(requestInfo,
            expectedResourceTypes))
        .build()
            .verify();
  }

  private static Builder createDefaultBuilder(ElementWrapper requestInfo,
      ResourceRequest resourceRequest) {
    return new ResourceRequestsVerifications.Builder()
            .withRequest(resourceRequest)
            .withRequestInfo(requestInfo);
  }

  private static Map<String, Long> extractActualCustomResourceTypes(
      ElementWrapper requestInfo, List<String> expectedResourceTypes) {
    ElementWrapper capability = requestInfo.getChild("capability");

    return extractCustomResourceTypes(capability,
        Sets.newHashSet(expectedResourceTypes));
  }

  private void verify() {
    assertEquals("nodeLabelExpression doesn't match",
        resourceRequest.getNodeLabelExpression(),
        requestInfo.getString("nodeLabelExpression"));
    assertEquals("numContainers doesn't match",
        resourceRequest.getNumContainers(),
        (int)requestInfo.getInt("numContainers"));
    assertEquals("relaxLocality doesn't match",
        resourceRequest.getRelaxLocality(),
        requestInfo.getBoolean("relaxLocality"));
    assertEquals("priority does not match",
        resourceRequest.getPriority().getPriority(),
        (int)requestInfo.getInt("priority"));
    assertEquals("resourceName does not match",
        resourceRequest.getResourceName(),
        requestInfo.getString("resourceName"));
    assertEquals("memory does not match",
        resourceRequest.getCapability().getMemorySize(),
        (long)requestInfo.getChild("capability").getLong("memory"));
    assertEquals("vCores does not match",
        resourceRequest.getCapability().getVirtualCores(),
        (long)requestInfo.getChild("capability").getLong("vCores"));

    verifyAtLeastOneCustomResourceIsSerialized();

    ElementWrapper executionTypeRequest =
        requestInfo.getChild("executionTypeRequest");
    assertEquals("executionType does not match",
        resourceRequest.getExecutionTypeRequest().getExecutionType().name(),
        executionTypeRequest.getString("executionType"));
    assertEquals("enforceExecutionType does not match",
        resourceRequest.getExecutionTypeRequest().getEnforceExecutionType(),
        executionTypeRequest.getBoolean("enforceExecutionType"));
  }

  /**
   * JSON serialization produces "invalid JSON" by default as maps are
   * serialized like this:
   * "customResources":{"entry":{"key":"customResource-1","value":"0"}}
   * If the map has multiple keys then multiple entries will be serialized.
   * Our json parser in tests cannot handle duplicates therefore only one
   * custom resource will be in the parsed json. See:
   * https://issues.apache.org/jira/browse/YARN-7505
   */
  private void verifyAtLeastOneCustomResourceIsSerialized() {
    boolean resourceFound = false;
    for (String expectedCustomResourceType : expectedCustomResourceTypes) {
      if (customResourceTypes.containsKey(expectedCustomResourceType)) {
        resourceFound = true;
        Long resourceValue =
            customResourceTypes.get(expectedCustomResourceType);
        assertNotNull("Resource value should not be null!", resourceValue);
      }
    }
    assertTrue("No custom resource type can be found in the response!",
        resourceFound);
  }

  /**
   * Builder class for {@link ResourceRequestsVerifications}.
   */
  public static final class Builder {
    private List<String> expectedCustomResourceTypes = Lists.newArrayList();
    private Map<String, Long> customResourceTypes;
    private ResourceRequest resourceRequest;
    private ElementWrapper requestInfo;

    Builder() {
    }

    public static Builder create() {
      return new Builder();
    }

    Builder withExpectedCustomResourceTypes(
            List<String> expectedCustomResourceTypes) {
      this.expectedCustomResourceTypes = expectedCustomResourceTypes;
      return this;
    }

    Builder withCustomResourceTypes(
            Map<String, Long> customResourceTypes) {
      this.customResourceTypes = customResourceTypes;
      return this;
    }

    Builder withRequest(ResourceRequest resourceRequest) {
      this.resourceRequest = resourceRequest;
      return this;
    }

    Builder withRequestInfo(ElementWrapper requestInfo) {
      this.requestInfo = requestInfo;
      return this;
    }

    public ResourceRequestsVerifications build() {
      return new ResourceRequestsVerifications(this);
    }
  }
}
