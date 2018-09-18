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

package org.apache.hadoop.yarn.server.resourcemanager.webapp.representationhelper.path;

import java.util.regex.Matcher;

/**
 * An implementation of {@link PathSegment} that represents a concrete element
 * of an array.
 */
public class ArrayElementPathSegment extends PathSegment {
  private final int index;

  ArrayElementPathSegment(String segmentName, String pathPart) {
    super(segmentName);
    this.index = extractArrayElementIndex(pathPart);
  }

  private int extractArrayElementIndex(String pathPart) {
    int arrayElementIndex = extractIndex(pathPart);
    if (arrayElementIndex >= 0) {
      return arrayElementIndex;
    } else {
      throw new IllegalStateException(
          "Array arrayElementIndex should be greater than 0!");
    }
  }

  private int extractIndex(String pathPart) {
    final Matcher matcher = ARRAY_ELEMENT_PATTERN.matcher(pathPart);

    // this call is intentional, without this,
    // matcher groups were empty even if matched!
    matcher.matches();
    try {
      return Integer.parseInt(matcher.group(1));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Cannot parse array element index: " + matcher.group());
    }
  }

  public int getIndex() {
    return index;
  }

  @Override
  PathSegmentType getType() {
    return PathSegmentType.ARRAY_ELEMENT;
  }

  @Override
  public String toString() {
    return String.format("%s[%d]", getName(), index);
  }
}
