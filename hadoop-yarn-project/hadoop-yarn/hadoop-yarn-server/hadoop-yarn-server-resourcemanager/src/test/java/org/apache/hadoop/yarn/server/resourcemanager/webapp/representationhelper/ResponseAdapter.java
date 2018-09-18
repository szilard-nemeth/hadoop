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
    .representationhelper.json.JsonChildNotFoundException;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.representationhelper.path.ArrayElementPathSegment;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.representationhelper.path.Path;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.representationhelper.path.PathSegment;

import org.apache.hadoop.yarn.server.resourcemanager.webapp
    .representationhelper.xml.XmlChildNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is intended to abstract away the semantics of JSON / XML parser
 * libraries, this is done by wrapping the normal elements
 * ({@link org.codehaus.jettison.json.JSONObject} / {@link org.w3c.dom.Element}
 * and array-typed elements {@link org.codehaus.jettison.json.JSONArray} /
 * {@link org.w3c.dom.NodeList}.
 *
 * The two public methods
 * {@link ResponseAdapter#getElement(String)} and
 * {@link ResponseAdapter#getArray(String)} are capable of parsing a path that
 * describes the hierarchy of the JSON / XML elements.
 *
 * Example of a path:
 * "scheduler.schedulerInfo.rootQueue.childQueues.queue[1].childQueues.queue[]"
 * which is the equivalent of this expression with Jettison:
 *
 * <pre>
 * JSONArray childQueueInfo =
 *     json.getJSONObject("scheduler")
 *     .getJSONObject("schedulerInfo")
 *     .getJSONObject("rootQueue").getJSONObject("childQueues")
 *     // get the first queue from the array
 *     .getJSONArray("queue").getJSONObject(1).getJSONObject("childQueues")
 *     .getJSONArray("queue");
 * </pre>
 *
 * What this class gives more than the expression above is that
 * it checks whether the child element names are found for every parent
 * and it also checks that the lengths of the specified arrays contains
 * at least as many elements as the queried array element index + 1.
 *
 * Please note that indexing of the arrays is zero based (developer friendly).
 */
public abstract class ResponseAdapter {
  private static final Logger LOG =
      LoggerFactory.getLogger(ResponseAdapter.class);

  public abstract ElementWrapper getElement(String pathStr);
  public abstract ArrayWrapper getArray(String pathStr);
  
  protected ElementWrapper getElementInternal(Path path) {
    if (path.isLastSegmentAnArray()) {
      throw new IllegalArgumentException(
          "Last segment of part is an array, getArray() should be invoked " +
                  "instead of getElement() method!");
    }
    return (ElementWrapper) getElementInternal(createWrapper(), path);
  }

  protected ArrayWrapper getArrayInternal(Path path) {
    if (!path.isLastSegmentAnArray()) {
      throw new IllegalArgumentException(
          "Last segment of part is a simple element, getElement() " +
                  "should be invoked instead of getArray method!");
    }
    return (ArrayWrapper) getElementInternal(createWrapper(), path);
  }

  private Wrapper getElementInternal(Wrapper root, Path path) {
    LOG.debug("Path processed so far: '{}'", path.getProcessedPath());

    final PathSegment pathSegment = path.getNextSegment();
    final String child = pathSegment.getName();

    final String processedPath = path.getProcessedPath();
    LOG.debug("Processing element: {}", processedPath);

    final Wrapper wrapper;
    checkChildFound(root, child, processedPath);
    if (pathSegment.isArrayElementType()) {
      final ArrayWrapper array = root.getChildArray(child);
      final int elementIndex = ((ArrayElementPathSegment)pathSegment)
              .getIndex();
      checkArrayIndexIsWithinBounds(array, elementIndex, processedPath);
      wrapper = array.getObjectAtIndex(elementIndex);
    } else if (pathSegment.isArrayType()) {
      wrapper = root.getChildArray(child);
    } else {
      wrapper = root.getChild(child);
    }

    if (!path.isLastSegment()) {
      return getElementInternal(wrapper, path);
    } else {
      return wrapper;
    }
  }

  private void checkArrayIndexIsWithinBounds(ArrayWrapper array,
      int arrayElementIndex, String processedPath) {
    final String message =
        String.format(
            "Array element %s does not have expected number of elements: %d"
                + " at path: %s",
            array, (arrayElementIndex + 1), processedPath);
    if (array.length() < (arrayElementIndex + 1)) {
      throw new IndexOutOfBoundsException(message);
    }
  }

  private void checkChildFound(Wrapper root, String child,
      String processedPath) {
    final String message =
        String.format("Element %s does not have child element: %s at path: %s",
            root, child, processedPath);
    if (!root.hasChild(child)) {
      if (getType() == RepresentationType.XML) {
        throw new XmlChildNotFoundException(message);
      } else if (getType() == RepresentationType.JSON) {
        throw new JsonChildNotFoundException(message);
      }
    }
  }

  public abstract Wrapper createWrapper();
  public abstract RepresentationType getType();

}
