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

package org.apache.hadoop.yarn.server.resourcemanager.webapp.representationhelper.json;

import org.apache.hadoop.yarn.server.resourcemanager.webapp.representationhelper.ArrayWrapper;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.representationhelper.ElementWrapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestJsonObjectWrapper {
  private static final String JSON =
          "{\"array\": [{ \"f1\": 11}, { \"f2\": 22}]," +
                  "\"longField\": 1234567890123345," +
                  "\"intField\": 12345," +
                  "\"floatField\": 2.55," +
                  "\"doubleField\": 2.555555," +
                  "\"stringField\": \"blabla\"," +
                  "\"booleanField\": \"false\"," +
                  "\"someObj\": { \"f1\": \"val1\"}" + 
                  "}";
  private static final double FLOAT_DELTA = 0.000001;

  private JsonObjectWrapper jsonObjectWrapper;

  @Before
  public void setUp() throws JSONException {
    JSONObject jsonObject = new JSONObject(JSON);
    jsonObjectWrapper = new JsonObjectWrapper(jsonObject);
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testWithNullArgument() {
    new JsonObjectWrapper(null);
  }

  @Test
  public void testLength() {
    assertEquals(8, jsonObjectWrapper.length());
  }

  @Test
  public void testHasChildNotExisting() {
    assertFalse(jsonObjectWrapper.hasChild("invalid"));
  }

  @Test
  public void testHasChildExisting() {
    assertTrue(jsonObjectWrapper.hasChild("array"));
  }

  @Test(expected = JsonChildNotFoundException.class)
  public void testGetChildNotExisting() {
    jsonObjectWrapper.getChild("invalid");
  }

  @Test(expected = JsonChildNotFoundException.class)
  public void testGetChildCalledWithArray() {
    jsonObjectWrapper.getChild("array");
  }

  @Test(expected = JsonChildNotFoundException.class)
  public void testGetChildCalledWithField() {
    jsonObjectWrapper.getChild("longField");
  }

  @Test
  public void testGetChildExisting() {
    ElementWrapper wrapper = jsonObjectWrapper.getChild("someObj");
    assertEquals("{\"f1\":\"val1\"}", wrapper.toString());
  }

  @Test(expected = JsonChildNotFoundException.class)
  public void testGetChildArrayNotExisting() {
    jsonObjectWrapper.getChildArray("invalid");
  }

  @Test(expected = JsonChildNotFoundException.class)
  public void testGetChildArrayCalledWithObject() {
    jsonObjectWrapper.getChildArray("someObj");
  }

  @Test(expected = JsonChildNotFoundException.class)
  public void testGetChildArrayCalledWithField() {
    jsonObjectWrapper.getChildArray("longField");
  }

  @Test
  public void testGetChildArrayExisting() {
    ArrayWrapper wrapper = jsonObjectWrapper.getChildArray("array");
    assertEquals("[{\"f1\":11},{\"f2\":22}]", wrapper.toString());
  }
  
  @Test
  public void testGetInt() {
    assertEquals(12345, (int) jsonObjectWrapper.getInt("intField"));
  }

  @Test
  public void testGetFloat() {
    assertEquals(2.55, jsonObjectWrapper.getFloat("floatField"), FLOAT_DELTA);
  }

  @Test
  public void testGetLong() {
    assertEquals(1234567890123345L, (long) jsonObjectWrapper.getLong("longField"));
  }

  @Test
  public void testGetDouble() {
    assertEquals(2.555555, jsonObjectWrapper.getDouble("doubleField"), FLOAT_DELTA);
  }

  @Test
  public void testGetString() {
    assertEquals("blabla", jsonObjectWrapper.getString("stringField"));
  }

  @Test
  public void testGetStringSafelyExisting() {
    assertEquals("blabla", jsonObjectWrapper.getStringSafely("stringField"));
  }

  @Test
  public void testGetStringSafelyNotExisting() {
    assertNull(jsonObjectWrapper.getStringSafely("xx"));
  }

  @Test
  public void testGetBoolean() {
    assertFalse(jsonObjectWrapper.getBoolean("booleanField"));
  }

  @Test
  public void testOptExisting() {
    assertFalse(Boolean.valueOf((String) jsonObjectWrapper.opt("booleanField")));
  }

  @Test
  public void testOptNotExisting() {
    assertNull(jsonObjectWrapper.opt("xx"));
  }

}