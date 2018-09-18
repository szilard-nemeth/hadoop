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


import com.google.common.collect.Lists;
import org.apache.hadoop.yarn.server.resourcemanager.webapp
    .representationhelper.RepresentationType;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.representationhelper
    .ResponseAdapter;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

/**
 * This class is a generic definition of an XML / JSON path. See explanation for
 * {@link ResponseAdapter}
 * for details about why this is useful in tests. A Path is basically a list of
 * {@link PathSegment}s. On top of this, this class has helper methods that can
 * process the path sequentially, and always knows what is the current
 * {@link PathSegment} which is being processed.
 */
public class Path {
  private final List<PathSegment> segments;
  private int currentIndex;
  private final boolean lastSegmentArray;

  private Path(List<PathSegment> pathSegments) {
    this.segments = pathSegments;
    this.currentIndex = 0;
    this.lastSegmentArray =
        pathSegments.get(pathSegments.size() - 1).isArrayType();
  }

  public static Path create(String path, RepresentationType type) {
    if (type == RepresentationType.JSON) {
      return createNormalPath(path);
    } else if (type == RepresentationType.XML) {
      return createXmlPath(path);
    } else {
      throw new IllegalArgumentException(
              "RepresentationType should be either XML or JSON!");
    }
  }

  private static Path createNormalPath(String path) {
    final List<PathSegment> pathSegments;
    if (path.contains(".")) {
      String[] pathParts = path.split("\\.");

      pathSegments = Lists.newArrayList();
      PathSegment lastPathSegment = null;
      for (String pathPart : pathParts) {
        PathSegment newPathSegment = validatePathPart(path, pathPart);
        validatePathSegment(lastPathSegment, newPathSegment);
        pathSegments.add(newPathSegment);
        lastPathSegment = newPathSegment;
      }
    } else if (!path.isEmpty()) {
      pathSegments = Lists.newArrayList(PathSegment.create(path));
    } else {
      throw new IllegalArgumentException(
              "Cannot create Path from an empty-string!");
    }
    return new Path(pathSegments);
  }

  private static void validatePathSegment(PathSegment lastPathSegment, 
      PathSegment newPathSegment) {
    if (lastPathSegment != null && lastPathSegment.isArrayType()
            && newPathSegment.isNormalType()) {
      throw new InvalidPathSegmentException(
              "Normal type cannot follow array type!");
    }
  }

  private static PathSegment validatePathPart(String path, String pathPart) {
    if (pathPart.isEmpty()) {
      throw new InvalidPathSegmentException(
              "Path should not contain empty segments: " + path);
    }
    return PathSegment.create(pathPart);
  }

  private static Path createXmlPath(String path) {
    //scheduler.schedulerInfo.rootQueue.childQueues.queue[5].childQueues.queue[] -->
    //scheduler.schedulerInfo.rootQueue.childQueues[5].childQueues[]
    final List<PathSegment> pathSegments;
    if (path.contains(".")) {
      String[] pathParts = path.split("\\.");
      pathSegments = Lists.newArrayList();
      int currentIdx = 0;
      PathSegment lastPathSegment = null;
      for (String pathPart : pathParts) {
        PathSegment newPathSegment = validatePathPart(path, pathPart);
        validatePathSegment(lastPathSegment, newPathSegment);
        if (newPathSegment.isArrayType()
            || newPathSegment.isArrayElementType() && currentIdx > 0) {
          shiftArrayPropertiesToPreviousSegment(pathSegments, currentIdx, 
              newPathSegment);
        } else {
          pathSegments.add(newPathSegment);
          currentIdx++;
        }
        lastPathSegment = newPathSegment;
      }
    } else if (!path.isEmpty()) {
      pathSegments = Lists.newArrayList(PathSegment.create(path));
    } else {
      throw new IllegalArgumentException(
          "Cannot create Path from an empty-string!");
    }
    return new Path(pathSegments);
  }

  private static void shiftArrayPropertiesToPreviousSegment(List<PathSegment>
      pathSegments, int currentIdx, PathSegment newPathSegment) {
    PathSegment prevSegment = pathSegments.get(currentIdx - 1);
    PathSegment replaced = PathSegment
        .createFrom(prevSegment.getName(), newPathSegment);
    pathSegments.remove(currentIdx - 1);
    pathSegments.add(replaced);
  }

  public PathSegment getNextSegment() {
    if (segments.size() == currentIndex) {
      throw new NoSuchElementException("Segment at index " + currentIndex + " is not found!");
    }
    return segments.get(currentIndex++);
  }

  public boolean isLastSegment() {
    return segments.size() == currentIndex;
  }

  public boolean isLastSegmentAnArray() {
    return lastSegmentArray;
  }

  public String getProcessedPath() {
    final List<PathSegment> subList = segments.subList(0, currentIndex);
    return subList.stream().map(PathSegment::toString)
        .collect(Collectors.joining("."));
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < segments.size(); ++i) {
      sb.append(segments.get(i).toString());
      if (i != segments.size() -1) {
        sb.append(".");
      }
    }
    return sb.toString();
  }
}
