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

import org.apache.hadoop.yarn.server.resourcemanager.webapp
    .representationhelper.ArrayWrapper;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.representationhelper.BufferedClientResponse;


import org.apache.hadoop.yarn.server.resourcemanager.webapp
    .representationhelper.ElementWrapper;
import org.apache.hadoop.yarn.server.resourcemanager.webapp
    .representationhelper.RepresentationType;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.representationhelper.ResponseAdapter;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.representationhelper.Wrapper;
import org.apache.hadoop.yarn.server.resourcemanager.webapp
    .representationhelper.path.Path;
import org.codehaus.jettison.json.JSONObject;

/**
 * A wrapper for a response that is converted to a {@link JSONObject}.
 */
public class JsonResponseAdapter extends ResponseAdapter {
  private JSONObject response;

  public JsonResponseAdapter(BufferedClientResponse response) {
    this.response = response.getEntity(JSONObject.class);
  }

  public ElementWrapper getElement(String pathStr) {
    return getElementInternal(Path.create(pathStr, getType()));
  }

  @Override
  public ArrayWrapper getArray(String pathStr) {
    return getArrayInternal(Path.create(pathStr, getType()));
  }

  @Override
  public Wrapper createWrapper() {
    return new JsonObjectWrapper(response);
  }

  @Override
  public RepresentationType getType() {
    return RepresentationType.JSON;
  }
}
