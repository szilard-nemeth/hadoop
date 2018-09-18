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
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;

/**
 * A wrapper for a JSON array.
 */
public class JsonArrayWrapper implements ArrayWrapper {
  private JSONArray jsonArray;

  JsonArrayWrapper(JSONArray jsonArray) {
    if (jsonArray == null) {
      throw new IllegalArgumentException("jsonArray should not be null!");
    }
    this.jsonArray = jsonArray;
  }

  @Override
  public int length() {
    return jsonArray.length();
  }

  @Override
  public ElementWrapper getObjectAtIndex(int idx) {
    try {
      return new JsonObjectWrapper(jsonArray.getJSONObject(idx));
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean hasChild(String child) {
    throw new UnsupportedOperationException(
            "hasChild is an unsupported operation on JsonArrays");
  }

  @Override
  public ElementWrapper getChild(String child) {
    throw new UnsupportedOperationException(
        "getChild is an unsupported operation on JsonArrays");
  }

  @Override
  public ArrayWrapper getChildArray(String child) {
    throw new UnsupportedOperationException(
        "getChildArray is an unsupported operation on JsonArrays"
    );
  }

  @Override
  public String toString() {
    return jsonArray.toString();
  }
}
