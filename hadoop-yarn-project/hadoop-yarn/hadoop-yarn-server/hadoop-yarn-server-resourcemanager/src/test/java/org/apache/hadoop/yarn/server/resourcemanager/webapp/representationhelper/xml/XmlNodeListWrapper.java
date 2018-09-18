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


import com.google.common.collect.Lists;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.representationhelper.ArrayWrapper;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.representationhelper.ElementWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.transform.TransformerException;
import java.util.List;

import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.getChildNodes;
import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.getElementsByTagName;
import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.toXml;


/**
 * A wrapper for an XML nodelist.
 */
public class XmlNodeListWrapper implements ArrayWrapper {
  private static final Logger LOG = LoggerFactory.getLogger(XmlNodeListWrapper.class);
  
  private List<ElementWrapper> elements;
  private NodeList nodes;

  private XmlNodeListWrapper(List<ElementWrapper> elements, NodeList nodes) {
    ensureNodesIsNotNull(nodes);
    this.elements = elements;
    this.nodes = nodes;
  }

  public static XmlNodeListWrapper create(Node node, String arrayName) {
    ensureArgumentsAreValid(node, arrayName);
    NodeList nodes = getElementsByTagName(node, arrayName);
    if (nodes.getLength() == 0) {
      throw new XmlChildNotFoundException("Child with name " + arrayName
          + " is not found! XML contents: " + toXml(node));
    }
    NodeList arrayNodes = getChildNodes(nodes.item(0));
    if (arrayNodes.getLength() == 0) {
      throw new XmlChildNotFoundException("Child with name " + arrayName
          + " is not an array! XML contents: " + toXml(node));
    }

    List<ElementWrapper> elements = Lists.newArrayList();
    for (int i = 0; i < arrayNodes.getLength(); i++) {
      Node child = arrayNodes.item(i);
      elements.add(new XmlNodeWrapper(child));
    }
    return new XmlNodeListWrapper(elements, arrayNodes);
  }

  private static void ensureArgumentsAreValid(Node node, String arrayName) {
    if (node == null) {
      throw new IllegalArgumentException("Node cannot be null!");
    }
    if (arrayName ==  null) {
      throw new IllegalArgumentException("Array name cannot be null!");
    }
    LOG.debug("Creating XmlNodeListWrapper, Node: " + node.toString()
            + ", arrayName: " + arrayName);
  }

  private void ensureNodesIsNotNull(NodeList nodes) {
    if (nodes == null) {
      throw new IllegalArgumentException("Node list cannot be null!");
    }
  }

  @Override
  public int length() {
    return elements.size();
  }

  @Override
  public ElementWrapper getObjectAtIndex(int idx) {
    return elements.get(idx);
  }

  @Override
  public boolean hasChild(String child) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ElementWrapper getChild(String child) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ArrayWrapper getChildArray(String child) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String toString() {
    try {
      return toXml(nodes);
    } catch (TransformerException e) {
      throw new RuntimeException(e);
    }
  }
}
