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
import java.util.regex.Pattern;

/**
 * A segment of a path.
 * There are three types of segments,
 * see implementations of {@link PathSegment}.
 * The type is determined by the string representation of segments,
 * for example: the array type does match for the ARRAY_PATTERN.
 */
public abstract class PathSegment {

  private static final Pattern ARRAY_PATTERN = Pattern.compile(".*\\[\\]");
  static final Pattern ARRAY_ELEMENT_PATTERN =
      Pattern.compile(".*\\[(\\d+)\\]");

  private final String name;

  PathSegment(String name) {
    this.name = name;
  }

  static PathSegment create(final String pathPart) {
    final PathSegmentType type = determineType(pathPart);
    final String segmentName = extractSegmentName(type, pathPart);

    if (type == PathSegmentType.ARRAY_ELEMENT) {
      return new ArrayElementPathSegment(segmentName, pathPart);
    } else if (type == PathSegmentType.ARRAY) {
      return new ArrayPathSegment(segmentName);
    } else if (type == PathSegmentType.NORMAL &&
        !pathPart.contains("[") &&
        !pathPart.contains("]")) {
      return new NormalPathSegment(segmentName);
    } else {
      throw new IllegalArgumentException(
              "Cannot parse array element index for segment: " + pathPart);
    }
  }
  
  static PathSegment createFrom(String segmentName, PathSegment other) {
    if (other.isArrayType()) {
      return PathSegment.create(segmentName + "[]");
    } else if (other.isArrayElementType()) {
      ArrayElementPathSegment arrayElementPathSegment =
          (ArrayElementPathSegment) other;
      int idx = arrayElementPathSegment
          .getIndex();
      return PathSegment.create(segmentName + "[" + idx +"]");
    } else {
      throw new IllegalArgumentException("This method should be invoked with " +
          "either array or array element typed PathSegments!");
    }
  }

  private static PathSegmentType determineType(String pathPart) {
    final boolean arrayType = ARRAY_PATTERN.matcher(pathPart).matches();
    final Matcher arrayElementMatcher =
            ARRAY_ELEMENT_PATTERN.matcher(pathPart);

    if (arrayElementMatcher.matches()) {
      return PathSegmentType.ARRAY_ELEMENT;
    } else if (arrayType) {
      return PathSegmentType.ARRAY;
    } else {
      return PathSegmentType.NORMAL;
    }
  }

  private static String extractSegmentName(
          PathSegmentType type, String pathPart) {
    if (type == PathSegmentType.ARRAY_ELEMENT ||
            type == PathSegmentType.ARRAY) {
      return pathPart.substring(0, pathPart.indexOf('['));
    } else {
      return pathPart;
    }
  }

  abstract PathSegmentType getType();

  public String getName() {
    return name;
  }

  public boolean isArrayType() {
    return getType() == PathSegmentType.ARRAY;
  }

  public boolean isArrayElementType() {
    return getType() == PathSegmentType.ARRAY_ELEMENT;
  }

  public boolean isNormalType() {
    return getType() == PathSegmentType.NORMAL;
  }
}
