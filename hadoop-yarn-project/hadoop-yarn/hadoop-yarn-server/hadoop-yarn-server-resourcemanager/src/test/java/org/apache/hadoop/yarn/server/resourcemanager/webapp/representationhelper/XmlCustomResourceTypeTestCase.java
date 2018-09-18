/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.webapp.representationhelper;

import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.representationhelper.xml.XmlResponseAdapter;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import javax.ws.rs.core.MediaType;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringReader;
import java.util.function.Consumer;

import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.toXml;
import static org.junit.Assert.assertEquals;

/**
 * This class hides the implementation details of how to verify the structure of
 * XML responses. Tests should only provide the path of the
 * {@link WebResource}, the response from the resource and
 * the verifier Consumer to
 * {@link XmlCustomResourceTypeTestCase#verify(Consumer)}. An instance of
 * {@link JSONObject} will be passed to that consumer to be able to
 * verify the response.
 */
public class XmlCustomResourceTypeTestCase {
  private static final Logger LOG =
      LoggerFactory.getLogger(XmlCustomResourceTypeTestCase.class);

  private WebResource path;
  private BufferedClientResponse response;
  private Document parsedResponse;

  public XmlCustomResourceTypeTestCase(WebResource path,
      ClientResponse response) {
    this.path = path;
    this.response = new BufferedClientResponse(response);
    this.parsedResponse = parseXml(this.response);
  }

  public void verify(Consumer<ResponseAdapter> verifier) {
    assertEquals(MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8,
        response.getType().toString());

    logResponse(parsedResponse);

    String responseStr = response.getEntity(String.class);
    if (responseStr == null || responseStr.isEmpty()) {
      throw new IllegalStateException("Response is null or empty!");
    }
    XmlResponseAdapter responseAdapter = new XmlResponseAdapter(response);
    verifier.accept(responseAdapter);
  }

  private Document parseXml(BufferedClientResponse response) {
    try {
      String xml = response.getEntity(String.class);
      DocumentBuilder db =
          DocumentBuilderFactory.newInstance().newDocumentBuilder();
      InputSource is = new InputSource();
      is.setCharacterStream(new StringReader(xml));
      return db.parse(is);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void logResponse(Document doc) {
    String responseStr = response.getEntity(String.class);
    LOG.info("Raw response from service URL {}: {}", path.toString(),
        responseStr);
    LOG.info("Parsed response from service URL {}: {}", path.toString(),
        toXml(doc));
  }
}
