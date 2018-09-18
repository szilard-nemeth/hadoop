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



import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringReader;

import static org.junit.Assert.assertEquals;

public class TestXmlNodeListWrapper {
  private static final String XML =
      "<childQueues>" +
          "<queue>" +
            "<f1>11</f1>" +
          "</queue>" +
          "<queue>" +
            "<f2>22</f2>" +
          "</queue>" + 
       "</childQueues>";
  
  private XmlNodeListWrapper wrapper;

  @Before
  public void setUp() {
    Document doc = parseXml(XML);
    wrapper = XmlNodeListWrapper.create(doc, "childQueues");
  }

  private Document parseXml(String xml) {
    try {
      DocumentBuilder db = DocumentBuilderFactory.newInstance()
          .newDocumentBuilder();
      InputSource is = new InputSource();
      is.setCharacterStream(new StringReader(xml));
      return db.parse(is);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private String removeWhiteSpaces(String s) {
    return s.replaceAll("\\s", "");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWithNullNodeArgument() {
    XmlNodeListWrapper.create(null, "dummy");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWithNullChildNameArgument() {
    Document doc = parseXml(XML);
    XmlNodeListWrapper.create(doc, null);
  }
  
  @Test
  public void testLength() {
    assertEquals(2, wrapper.length());
  }

  @Test
  public void testGetObjectAtIndex() {
    assertEquals("<queue><f1>11</f1></queue>",
            removeWhiteSpaces(wrapper.getObjectAtIndex(0).toString()));
    assertEquals("<queue><f2>22</f2></queue>",
            removeWhiteSpaces(wrapper.getObjectAtIndex(1).toString()));
  }

  @Test(expected = RuntimeException.class)
  public void testGetObjectAtIndexOutOfRange() {
    wrapper.getObjectAtIndex(2);
  }
  
  @Test(expected = UnsupportedOperationException.class)
  public void testHasChild() {
    wrapper.hasChild("a");
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGetChild() {
    wrapper.getChild("a");
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGetChildArray() {
    wrapper.getChildArray("a");
  }
}
