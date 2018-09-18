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

package org.apache.hadoop.yarn.server.resourcemanager.webapp
    .representationhelper.xml;

import org.apache.hadoop.yarn.server.resourcemanager.webapp
    .representationhelper.BufferedClientResponse;
import org.junit.Test;
import org.w3c.dom.Document;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestXmlResponseAdapter {
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
  
  @Test
  public void testParse() {
    BufferedClientResponse mockResponse = mock(BufferedClientResponse.class);
    when(mockResponse.getEntity(String.class)).thenReturn(XML);
    XmlResponseAdapter responseAdapter = new XmlResponseAdapter(mockResponse);
    Document parsedDocument = responseAdapter.getParsedResponse();
  }

}