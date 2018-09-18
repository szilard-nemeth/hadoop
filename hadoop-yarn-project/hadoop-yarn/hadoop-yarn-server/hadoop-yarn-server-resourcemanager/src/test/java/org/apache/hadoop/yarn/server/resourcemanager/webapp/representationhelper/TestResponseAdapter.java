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

package org.apache.hadoop.yarn.server.resourcemanager.webapp
    .representationhelper;

import com.sun.jersey.api.client.ClientResponse;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.representationhelper.json.JsonChildNotFoundException;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.representationhelper.json.JsonResponseAdapter;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.representationhelper.xml.XmlChildNotFoundException;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.representationhelper.xml.XmlNodeListWrapper;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.representationhelper.xml.XmlResponseAdapter;
import org.apache.log4j.LogManager;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;

import static org.apache.hadoop.yarn.server.resourcemanager.webapp.representationhelper.FileReaderTestHelper.loadFileAsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class TestResponseAdapter {
  private static String schedulerInfoJson = null;
  private static String schedulerInfoXml = null;

  static {
    try {
      schedulerInfoJson = loadFileAsString("scheduler-info.json");
      schedulerInfoXml = loadFileAsString("scheduler-info.xml");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private RepresentationType representationType;
  private ResponseAdapter responseAdapter;

  public TestResponseAdapter(RepresentationType representationType) {
    this.representationType = representationType;
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {RepresentationType.JSON},
        {RepresentationType.XML}
    });
  }
  
  @Before
  public void setUp() {
    InputStream is = createInputStream();
    if (representationType == RepresentationType.JSON) {
      responseAdapter = getJsonAdapter(is);
    } else if (representationType == RepresentationType.XML) {
      responseAdapter = getXmlAdapter(is);
    }
    LogManager.getLogger(
        XmlNodeListWrapper.class.getName()).setLevel(org.apache.log4j.Level.DEBUG);
  }

  private JsonResponseAdapter getJsonAdapter(InputStream is) {
    ClientResponse mockClientResponse = mock(ClientResponse.class);
    when(mockClientResponse.getEntityInputStream()).thenReturn(is);
    when(mockClientResponse.getEntity(JSONObject.class))
        .thenAnswer(invocation -> new JSONObject(schedulerInfoJson));

    BufferedClientResponse bufferedResponse = new
        BufferedClientResponse(mockClientResponse);
    return new JsonResponseAdapter(bufferedResponse);
  }

  private XmlResponseAdapter getXmlAdapter(InputStream is) {
    ClientResponse mockClientResponse = mock(ClientResponse.class);
    when(mockClientResponse.getEntityInputStream()).thenReturn(is);
    when(mockClientResponse.getEntity(String.class)).thenReturn(schedulerInfoXml);

    BufferedClientResponse bufferedResponse = new
        BufferedClientResponse(mockClientResponse);
    return new XmlResponseAdapter(bufferedResponse);
  }

  private InputStream createInputStream() {
    return new ByteArrayInputStream(schedulerInfoJson.getBytes(StandardCharsets.UTF_8));
  }

  private Class<?> getExpectedExceptionTypeChildNotFound() {
    if (representationType == RepresentationType.JSON) {
      return JsonChildNotFoundException.class;
    } else if (representationType == RepresentationType.XML) {
      return XmlChildNotFoundException.class;
    } else {
      throw new IllegalStateException("Representation type should be either XML or JSON!");
    }
  }

  @Test
  public void testInvalidObjectNameOnPath() {
    try {
      responseAdapter.getElement("schedulerr");
    } catch (Exception e) {
      Class<?> clazz = 
          getExpectedExceptionTypeChildNotFound();
      assertEquals("Expected exception should be of type " + clazz, clazz,
              e.getClass());
    }
  }

  @Test
  public void testInvalidObjectNameOnPath2() {
    try {
      responseAdapter.getElement("scheduler.blabla");
    } catch (Exception e) {
      Class<?> clazz =
          getExpectedExceptionTypeChildNotFound();
      assertEquals("Expected exception should be of type " + clazz, clazz,
          e.getClass());
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetObjectAsArray() {
    responseAdapter.getElement("scheduler[]");
  }

  @Test
  public void testGetArrayAsObject() {
    try {
      responseAdapter.getElement("scheduler"
          + ".schedulerInfo.rootQueue.childQueues.queue");
    } catch (Exception e) {
      Class<?> clazz =
          getExpectedExceptionTypeChildNotFound();
      assertEquals("Expected exception should be of type " + clazz, clazz,
          e.getClass());
    }
  }

  @Test
  public void testArray() {
    ElementWrapper element = responseAdapter.getElement("scheduler"
        + ".schedulerInfo.rootQueue.childQueues.queue[0]");
    assertNotNull(element);
    ArrayWrapper array = responseAdapter.getArray("scheduler"
        + ".schedulerInfo.rootQueue.childQueues.queue[1].childQueues.queue[]");
    assertNotNull(array);
    Assert.assertEquals(2, array.length());
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testArrayIndexOutOfBounds() {
    responseAdapter.getArray("scheduler"
        + ".schedulerInfo.rootQueue.childQueues.queue[5].childQueues.queue[]");
  }

  @Test
  public void testGetRootElement() {
    ElementWrapper scheduler = responseAdapter.getElement("scheduler");
    assertNotNull(scheduler);
  }

}