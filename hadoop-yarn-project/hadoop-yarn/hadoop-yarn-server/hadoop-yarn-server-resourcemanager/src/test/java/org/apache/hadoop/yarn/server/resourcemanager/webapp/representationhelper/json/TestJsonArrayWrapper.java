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


import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestJsonArrayWrapper {
  private static final String JSON_WITH_AN_ARRAY =
      "{\"array\": [{ \"f1\": 11}, { \"f2\": 22}]}";
  
  private JsonArrayWrapper jsonArrayWrapper;

  @Before
  public void setUp() throws JSONException {
    JSONObject jsonObject = new JSONObject(JSON_WITH_AN_ARRAY);
    JSONArray jsonArray = jsonObject.getJSONArray("array");
    jsonArrayWrapper = new JsonArrayWrapper(jsonArray);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWithNullArgument() {
    new JsonArrayWrapper(null);
  }
  
  @Test
  public void testLength() {
    assertEquals(2, jsonArrayWrapper.length());
  }

  @Test
  public void testGetObjectAtIndex() {
    assertEquals("{\"f1\":11}", jsonArrayWrapper.getObjectAtIndex(0).toString());
    assertEquals("{\"f2\":22}", jsonArrayWrapper.getObjectAtIndex(1).toString());
  }

  @Test(expected = RuntimeException.class)
  public void testGetObjectAtIndexOutOfRange() {
    jsonArrayWrapper.getObjectAtIndex(2);
  }
  
  @Test(expected = UnsupportedOperationException.class)
  public void testHasChild() {
    jsonArrayWrapper.hasChild("a");
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGetChild() {
    jsonArrayWrapper.getChild("a");
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGetChildArray() {
    jsonArrayWrapper.getChildArray("a");
  }
}
