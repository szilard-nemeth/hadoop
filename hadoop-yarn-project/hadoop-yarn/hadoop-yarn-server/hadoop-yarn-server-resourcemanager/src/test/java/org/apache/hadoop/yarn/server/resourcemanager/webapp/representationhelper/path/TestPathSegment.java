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

import org.junit.Test;

import static org.junit.Assert.*;

public class TestPathSegment {

  @Test
  public void testCreateNormalSegment() {
    PathSegment pathSegment = PathSegment.create("abc");
    assertEquals(PathSegmentType.NORMAL, pathSegment.getType());
    assertEquals("abc", pathSegment.getName());
    assertTrue(pathSegment.isNormalType());
    assertFalse(pathSegment.isArrayType());
    assertFalse(pathSegment.isArrayElementType());
  }

  @Test
  public void testCreateArraySegment() {
    PathSegment pathSegment = PathSegment.create("abc[]");
    assertEquals(PathSegmentType.ARRAY, pathSegment.getType());
    assertEquals("abc", pathSegment.getName());
    assertTrue(pathSegment.isArrayType());
    assertFalse(pathSegment.isNormalType());
    assertFalse(pathSegment.isArrayElementType());
  }

  @Test
  public void testCreateArrayElementSegment() {
    PathSegment pathSegment = PathSegment.create("abc[12]");
    assertEquals(PathSegmentType.ARRAY_ELEMENT, pathSegment.getType());
    assertEquals("abc", pathSegment.getName());
    assertEquals(12, ((ArrayElementPathSegment)pathSegment).getIndex());
    assertTrue(pathSegment.isArrayElementType());
    assertFalse(pathSegment.isNormalType());
    assertFalse(pathSegment.isArrayType());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateArrayElementSegmentWithLetterIndex() {
    PathSegment.create("abc[x]");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateArrayElementSegmentWithNegativeIndex() {
    PathSegment.create("abc[-1]");
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testCreateFromAnotherNormalSegment() {
    PathSegment otherSegment = PathSegment.create("other");
    PathSegment.createFrom("abc", otherSegment);
  }

  @Test
  public void testCreateFromAnotherArraySegment() {
    PathSegment otherSegment = PathSegment.create("other[]");
    PathSegment segment = PathSegment.createFrom("abc", otherSegment);
    assertEquals("abc", segment.getName());
    assertTrue(segment.isArrayType());
    assertFalse(segment.isNormalType());
    assertFalse(segment.isArrayElementType());
  }

  @Test
  public void testCreateFromAnotherArrayElementSegment() {
    PathSegment otherSegment = PathSegment.create("other[2]");
    PathSegment segment = PathSegment.createFrom("abc", otherSegment);
    assertEquals("abc", segment.getName());
    assertTrue(segment.isArrayElementType());
    assertFalse(segment.isArrayType());
    assertFalse(segment.isNormalType());
  }
}