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

import org.apache.hadoop.yarn.server.resourcemanager.webapp
    .representationhelper.RepresentationType;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import static org.apache.hadoop.yarn.server.resourcemanager.webapp
    .representationhelper.RepresentationType.JSON;

/**
 * A wrapper for a JSON object.
 */
public class JsonObjectWrapper implements ElementWrapper {
  private JSONObject jsonObject;

  public JsonObjectWrapper(JSONObject jsonObject) {
    if (jsonObject == null) {
      throw new IllegalArgumentException("jsonObject should not be null!");
    }
    this.jsonObject = jsonObject;
  }

  @Override
  public boolean hasChild(String child) {
    return jsonObject.has(child);
  }

  @Override
  public ElementWrapper getChild(String child) {
    try {
      return new JsonObjectWrapper(jsonObject.getJSONObject(child));
    } catch (JSONException e) {
      throw new JsonChildNotFoundException(e);
    }
  }

  @Override
  public ArrayWrapper getChildArray(String child) {
    try {
      return new JsonArrayWrapper(jsonObject.getJSONArray(child));
    } catch (JSONException e) {
      throw new JsonChildNotFoundException(e);
    }
  }

  @Override
  public int length() {
    return jsonObject.length();
  }

  @Override
  public Object opt(String child) {
    return jsonObject.opt(child);
  }

  @Override
  public Integer getInt(String key) {
    try {
      return jsonObject.getInt(key);
    } catch (JSONException e) {
      throw new JsonValueNotFoundException(e);
    }
  }

  @Override
  public Float getFloat(String key) {
    try {
      return (float) jsonObject.getDouble(key);
    } catch (JSONException e) {
      throw new JsonValueNotFoundException(e);
    }
  }

  @Override
  public Long getLong(String key) {
    try {
      return jsonObject.getLong(key);
    } catch (JSONException e) {
      throw new JsonValueNotFoundException(e);
    }
  }

  @Override
  public Double getDouble(String key) {
    try {
      return jsonObject.getDouble(key);
    } catch (JSONException e) {
      throw new JsonValueNotFoundException(e);
    }
  }

  @Override
  public String getString(String key) {
    try {
      return jsonObject.getString(key);
    } catch (JSONException e) {
      throw new JsonValueNotFoundException(e);
    }
  }

  @Override
  public String getStringSafely(String key) {
    try {
      return jsonObject.getString(key);
    } catch (JSONException e) {
      return null;
    }
  }

  @Override
  public Boolean getBoolean(String key) {
    try {
      return jsonObject.getBoolean(key);
    } catch (JSONException e) {
      throw new JsonValueNotFoundException(e);
    }
  }

  @Override
  public RepresentationType getRepresentationType() {
    return JSON;
  }

  public JSONObject getWrapped() {
    return jsonObject;
  }

  @Override
  public String toString() {
    if (jsonObject != null) {
      return jsonObject.toString();
    }
    return null;
  }
}
