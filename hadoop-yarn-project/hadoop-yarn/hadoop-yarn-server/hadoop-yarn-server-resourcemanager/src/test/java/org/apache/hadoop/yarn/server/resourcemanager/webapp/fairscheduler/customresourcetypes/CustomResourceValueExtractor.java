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

package org.apache.hadoop.yarn.server.resourcemanager.webapp.fairscheduler.customresourcetypes;


import com.google.common.collect.Maps;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceTypes;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.representationhelper.ArrayWrapper;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.representationhelper.ElementWrapper;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.representationhelper.JsonXmlWrapperPrinter;
import org.junit.Assert;

import java.util.Map;
import java.util.Set;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class CustomResourceValueExtractor {
  private static final String RESOURCE_INFORMATIONS = "resourceInformations";
  private static final String RESOURCE_INFORMATION = "resourceInformation";
  private static final String NAME = "name";
  private static final String RESOURCE_TYPE = "resourceType";
  private static final String UNITS = "units";
  private static final String VALUE = "value";

  public static Map<String, Long> extractCustomResourceTypes(
          ElementWrapper resourceCategory,
      Set<String> expectedResourceTypes) {
    Assert.assertTrue(
            getFieldNotFoundErrorMessage(resourceCategory, "resourceCategory",
                    RESOURCE_INFORMATIONS),
            resourceCategory.hasChild(RESOURCE_INFORMATIONS));
    ElementWrapper resourceInformations =
            resourceCategory.getChild(RESOURCE_INFORMATIONS);
    Assert.assertTrue(
            getFieldNotFoundErrorMessage(resourceCategory,
                    RESOURCE_INFORMATIONS, RESOURCE_INFORMATION),
            resourceInformations.hasChild(RESOURCE_INFORMATION));

    ArrayWrapper customResources =
        resourceInformations.getChildArray(RESOURCE_INFORMATION);

    // customResources will include vcores / memory as well
    assertEquals(
        "Different number of custom resource types found than expected",
        expectedResourceTypes.size(), customResources.length() - 2);

    Map<String, Long> resourceTypesAndValues = Maps.newHashMap();
    for (int i = 0; i < customResources.length(); i++) {
      ElementWrapper customResource = customResources.getObjectAtIndex(i);
      Assert.assertTrue(getFieldNotFoundErrorMessage(customResource,
              "Custom resource", NAME), customResource.hasChild(NAME));
      Assert.assertTrue(getFieldNotFoundErrorMessage(customResource,
              "Custom resource", RESOURCE_TYPE),
              customResource.hasChild(RESOURCE_TYPE));
      Assert.assertTrue(
          getFieldNotFoundErrorMessage(customResource,
              "Custom resource", UNITS),
          customResource.hasChild(UNITS));
      Assert.assertTrue(
          getFieldNotFoundErrorMessage(customResource,
              "Custom resource", VALUE),
          customResource.hasChild("value"));

      String name = customResource.getString(NAME);
      String unit = customResource.getString(UNITS);
      String resourceType = customResource.getString(RESOURCE_TYPE);
      Long value = customResource.getLong("value");

      if (ResourceInformation.MEMORY_URI.equals(name)
          || ResourceInformation.VCORES_URI.equals(name)) {
        continue;
      }

      assertTrue("Custom resource type " + name + " not found",
          expectedResourceTypes.contains(name));
      assertEquals("k", unit);
      assertEquals(ResourceTypes.COUNTABLE,
          ResourceTypes.valueOf(resourceType));
      assertNotNull(
              "Custom resource value should not be null for resource type "
                      + resourceType + ", listing xml contents: "
                      + JsonXmlWrapperPrinter.print(customResource),
              value);
      resourceTypesAndValues.put(name, value);
    }

    return resourceTypesAndValues;
  }
  
  private static String getFieldNotFoundErrorMessage(ElementWrapper wrapper, 
      String prefix, String field) {
    return String.format("%s object has no field with name %s in object: %s",
            prefix, field, JsonXmlWrapperPrinter.print(wrapper));
  }
}
