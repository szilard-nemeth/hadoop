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

package org.apache.hadoop.yarn.server.resourcemanager.webapp.representationhelper.xml;

import org.apache.hadoop.yarn.server.resourcemanager.webapp.representationhelper.ArrayWrapper;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.representationhelper.ElementWrapper;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.representationhelper.RepresentationType;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import static org.apache.hadoop.yarn.server.resourcemanager.webapp.representationhelper.RepresentationType.XML;


import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils
    .getElementsByTagName;
import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils
    .getLengthOfChildNodes;
import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.getXmlBoolean;
import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.getXmlFloat;
import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.getXmlInt;
import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.getXmlLong;
import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.getXmlString;
import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.toXml;

/**
 * A wrapper class for an XML node.
 */
public class XmlNodeWrapper implements ElementWrapper {
  private Node node;

  XmlNodeWrapper(Node node) {
    if (node == null) {
      throw new IllegalArgumentException("Node cannot be null!");
    }
    this.node = node;
  }

  @Override
  public boolean hasChild(String child) {
    return getElementsByTagName(node, child).getLength() > 0;
  }

  @Override
  public ElementWrapper getChild(String child) {
    NodeList nodes = getElementsByTagName(node, child);
    if (nodes == null || nodes.getLength() == 0) {
      String xml = toXml(node);
      throw new XmlChildNotFoundException(
          "Child " + child + " not found in xml: " + xml);
    }    
    return new XmlNodeWrapper(nodes.item(0));
  }

  @Override
  public ArrayWrapper getChildArray(String child) {
    return XmlNodeListWrapper.create(node, child);
  }

  @Override
  public int length() {
    return getLengthOfChildNodes(node);
  }

  @Override
  public Object opt(String child) {
    NodeList nodes = getElementsByTagName(node, child);
    if (nodes.getLength() > 0) {
      return nodes.item(0);
    }
    return null;
  }

  @Override
  public Integer getInt(String key) {
    return getXmlInt((Element) node, key);
  }

  @Override
  public Float getFloat(String key) {
    return getXmlFloat((Element) node, key);
  }

  @Override
  public Long getLong(String key) {
    return getXmlLong((Element) node, key);
  }

  @Override
  public Double getDouble(String key) {
    return (double) getXmlFloat((Element) node, key);
  }

  @Override
  public String getString(String key) {
    return getXmlString((Element) node, key);
  }

  @Override
  public String getStringSafely(String key) {
    return getString(key);
  }

  @Override
  public Boolean getBoolean(String key) {
    return getXmlBoolean((Element) node, key);
  }

  @Override
  public RepresentationType getRepresentationType() {
    return XML;
  }

  @Override
  public String toString() {
    if (node != null) {
      return toXml(node);
    }
    return null;
  }

  public Node getWrapped() {
    return node;
  }

}
