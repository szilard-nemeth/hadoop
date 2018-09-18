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
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestXmlNodeWrapper {
  private static final String XML =
  "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
      "<root>" +
        "<childQueues>" +
          "<queue>" +
            "<f1>11</f1>" +
          "</queue>" +
          "<queue>" +
            "<f2>22</f2>" +
          "</queue>" +
        "</childQueues>" +
        "<booleanField>false</booleanField>" +
        "<doubleField>2.555555</doubleField>" +
        "<floatField>2.55</floatField>" +
        "<intField>12345</intField>" +
        "<longField>1234567890123345</longField>" +
        "<someObj>" +
          "<f1>val1</f1>" +
        "</someObj>" +
        "<stringField>blabla</stringField>" +
      "</root>";

  private XmlNodeWrapper xmlWrapper;
  
  private void assertXmlContents(ElementWrapper element, String name, 
      String value) {
    String expected = "<" + name + ">" +
        value + "</" + name + ">";
    assertEquals(expected, removeWhiteSpaces(element.toString()));
  }
  
  private String removeWhiteSpaces(String s) {
    return s.replaceAll("\\s", "");
  }

  @Before
  public void setUp() {
    Document doc = parseXml(XML);
    xmlWrapper = new XmlNodeWrapper(doc);
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
  
  @Test(expected = IllegalArgumentException.class)
  public void testWithNullArgument() {
    new XmlNodeWrapper(null);
  }

  @Test
  public void testLength() {
    assertEquals(8, xmlWrapper.length());
  }

  @Test
  public void testHasChildNotExisting() {
    assertFalse(xmlWrapper.hasChild("invalid"));
  }

  @Test
  public void testHasChildExisting() {
    assertTrue(xmlWrapper.hasChild("childQueues"));
  }

  @Test(expected = XmlChildNotFoundException.class)
  public void testGetChildNotExisting() {
    xmlWrapper.getChild("invalid");
  }

  @Test
  public void testGetChildCalledWithArray() {
    ArrayWrapper array = xmlWrapper.getChildArray("childQueues");
    String f1Value = array.getObjectAtIndex(0).toString();
    String f2Value = array.getObjectAtIndex(1).toString();
    assertEquals("<queue><f1>11</f1></queue>", removeWhiteSpaces(f1Value));
    assertEquals("<queue><f2>22</f2></queue>", removeWhiteSpaces(f2Value));
  }

  @Test
  public void testGetChildCalledWithField() {
    ElementWrapper longField = xmlWrapper.getChild("longField");
    assertXmlContents(longField, "longField", "1234567890123345");
  }

  @Test
  public void testGetChildExisting() {
    ElementWrapper wrapper = xmlWrapper.getChild("someObj");
    assertXmlContents(wrapper, "someObj", "<f1>val1</f1>");
  }

  @Test(expected = XmlChildNotFoundException.class)
  public void testGetChildArrayNotExisting() {
    xmlWrapper.getChildArray("invalid");
  }

  @Test
  public void testGetChildArrayCalledWithObject() {
    xmlWrapper.getChildArray("someObj");
  }

  @Test(expected = XmlChildNotFoundException.class)
  public void testGetChildArrayCalledWithField() {
    xmlWrapper.getChildArray("longField");
  }

}