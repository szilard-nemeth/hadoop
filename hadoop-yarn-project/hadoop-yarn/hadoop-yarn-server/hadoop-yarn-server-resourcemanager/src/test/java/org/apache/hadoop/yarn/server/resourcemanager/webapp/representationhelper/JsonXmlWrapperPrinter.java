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


import org.apache.hadoop.yarn.server.resourcemanager.webapp
    .representationhelper.json.JsonObjectWrapper;
import org.apache.hadoop.yarn.server.resourcemanager.webapp
    .representationhelper.xml.XmlNodeWrapper;
import org.codehaus.jettison.json.JSONObject;
import org.w3c.dom.Node;

import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.toJson;
import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.toXml;

public class JsonXmlWrapperPrinter {
  
  public static String print(ElementWrapper wrapper) {
    RepresentationType representationType = wrapper.getRepresentationType();
    if (representationType == RepresentationType.XML) {
      XmlNodeWrapper xmlNodeWrapper = (XmlNodeWrapper) wrapper;
      Node node = xmlNodeWrapper.getWrapped();
      return toXml(node);
    } else if (representationType == RepresentationType.JSON) {
      JsonObjectWrapper jsonObjectWrapper = (JsonObjectWrapper) wrapper;

      JSONObject jsonObject = jsonObjectWrapper.getWrapped();
      return toJson(jsonObject);
    }
    return "";
  }
}
