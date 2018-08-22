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

package org.apache.hadoop.fs.ozone.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractGetFileStatusTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;

/**
 * Ozone contract tests covering getFileStatus.
 */
public class ITestOzoneContractGetFileStatus
    extends AbstractContractGetFileStatusTest {


  @BeforeClass
  public static void createCluster() throws IOException {
    OzoneContract.createCluster();
  }

  @AfterClass
  public static void teardownCluster() throws IOException {
    OzoneContract.destroyCluster();
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new OzoneContract(conf);
  }

  @Override
  public void teardown() throws Exception {
    getLogger().info("FS details {}", getFileSystem());
    super.teardown();
  }

  @Override
  protected Configuration createConfiguration() {
    return super.createConfiguration();
  }
}
