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

import org.apache.hadoop.yarn.server.resourcemanager.webapp.representationhelper.json
    .JsonObjectWrapper;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.representationhelper.xml
    .XmlNodeWrapper;

/**
 * This interface is intended to be extended by implementations of
 * representational classes. Currently there are two concrete implementations,
 * see:
 * {@link JsonObjectWrapper}
 * and
 * {@link XmlNodeWrapper}
 */
public interface ElementWrapper extends Wrapper {
  Object opt(String child);

  Integer getInt(String key);

  Long getLong(String key);

  Float getFloat(String key);

  Double getDouble(String key);

  String getString(String key);

  String getStringSafely(String key);

  Boolean getBoolean(String key);
  
  RepresentationType getRepresentationType();
}
