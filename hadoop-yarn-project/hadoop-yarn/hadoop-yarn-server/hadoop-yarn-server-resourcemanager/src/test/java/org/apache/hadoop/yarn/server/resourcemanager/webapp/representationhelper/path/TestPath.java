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
    .representationhelper.path;

import org.apache.hadoop.yarn.server.resourcemanager.webapp.representationhelper.RepresentationType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.NoSuchElementException;

import static org.apache.hadoop.yarn.server.resourcemanager.webapp
    .representationhelper.RepresentationType.JSON;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp.representationhelper.RepresentationType.XML;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class TestPath {

  private final RepresentationType representationType;

  public TestPath(RepresentationType representationType) {
    this.representationType = representationType;
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {JSON},
        {XML}
    });
  }

  private PathSegment assertSegment(Path path, int idx, String name) {
    PathSegment segment = path.getNextSegment();
    assertEquals(String.format("Segment with index %d should have name '%s'!",
            idx, name), name, segment.getName());
    return segment;
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateEmptyPath() {
    Path.create("", representationType);
  }

  @Test
  public void testCreatePathWithOneSegment() {
    Path path = Path.create("a", representationType);
    assertSegment(path, 0, "a");
  }

  @Test(expected = InvalidPathSegmentException.class)
  public void testCreatePathWithInvalidSegmentsInTheMiddle() {
    Path path = Path.create("a...c.d", representationType);
    assertSegment(path, 0, "a");
  }

  @Test
  public void testCreatePathWithNormalElements() {
    Path path = Path.create("a.b.c.d", representationType);
    assertSegment(path, 0, "a");
    assertSegment(path, 1, "b");
    assertSegment(path, 2, "c");
    assertSegment(path, 3, "d");
  }

  @Test
  public void testCreatePathWithNormalAndArrayElements() {
    Path path = Path.create("a.b.c.d[]", representationType);
    assertSegment(path, 0, "a");
    assertSegment(path, 1, "b");
    
    if (representationType == XML) {
      assertEquals("a.b.c[]", path.toString());
      PathSegment segmentC = assertSegment(path, 2, "c");
      assertTrue(segmentC.isArrayType());
    } else {
      assertEquals("a.b.c.d[]", path.toString());
      PathSegment segmentC = assertSegment(path, 2, "c");
      assertFalse(segmentC.isArrayType());
      PathSegment segmentD = assertSegment(path, 3, "d");
      assertTrue(segmentD.isArrayType());
    }
  }
  
  @Test
  public void testCreateComplexXmlPath() {
    if (representationType == XML) {
      Path path = Path.create("scheduler.schedulerInfo.rootQueue.childQueues" +
          ".queue[5].childQueues.queue[]", XML);
      assertEquals(
              "scheduler.schedulerInfo.rootQueue.childQueues[5].childQueues[]",
              path.toString());
    }
  }

  @Test(expected = InvalidPathSegmentException.class)
  public void testCreatePathWithArraySegmentFollowedByNormalSegment() {
    Path path = Path.create("a.b[].c", representationType);
    assertSegment(path, 0, "a");
    assertSegment(path, 1, "b");
    assertSegment(path, 2, "c");
  }

  @Test
  public void testCreatePathAndTryToGetNotExistingSegment() {
    Path path = Path.create("a.b.c.d", representationType);
    assertSegment(path, 0, "a");
    assertSegment(path, 1, "b");
    assertSegment(path, 2, "c");
    assertSegment(path, 3, "d");
    assertTrue(path.isLastSegment());

    boolean caught = false;
    try {
      path.getNextSegment();
    } catch (NoSuchElementException e) {
      caught = true;
    }
    assertTrue("Expected a NoSuchElementException with the getNextElement call "
            + "to path after all elements are read!", caught);
  }
  
  @Test
  public void testIsLastSegment() {
    Path path = Path.create("a.b.c.d", representationType);
    path.getNextSegment();
    assertFalse(path.isLastSegment());
    path.getNextSegment();
    assertFalse(path.isLastSegment());
    path.getNextSegment();
    assertFalse(path.isLastSegment());
    path.getNextSegment();
    assertTrue(path.isLastSegment());
  }
  
  @Test
  public void testIsLastSegmentAnArray() {
    Path path = Path.create("a.b.c.d", representationType);
    assertFalse(path.isLastSegmentAnArray());

    path = Path.create("a.b.c.d[]", representationType);
    assertTrue(path.isLastSegmentAnArray());
  }

  @Test
  public void testProcessedPath() {
    Path path = Path.create("a.b.c.d", representationType);
    assertEquals("", path.getProcessedPath());
    
    path.getNextSegment();
    assertEquals("a", path.getProcessedPath());
    path.getNextSegment();
    assertEquals("a.b", path.getProcessedPath());
    path.getNextSegment();
    assertEquals("a.b.c", path.getProcessedPath());
    path.getNextSegment();
    assertEquals("a.b.c.d", path.getProcessedPath());
    try {
      path.getNextSegment();
    } catch (Exception e) {
      //intentionally empty
    }
    assertEquals("a.b.c.d", path.getProcessedPath());
  }
}