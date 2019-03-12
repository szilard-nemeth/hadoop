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


package org.apache.hadoop.yarn.submarine.client.cli;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.resourcetypes.ResourceTypesTestHelper;
import org.apache.hadoop.yarn.submarine.client.cli.param.RunJobParameters;
import org.apache.hadoop.yarn.submarine.client.cli.param.yaml.YamlParseException;
import org.apache.hadoop.yarn.submarine.common.MockClientContext;
import org.apache.hadoop.yarn.submarine.common.conf.SubmarineLogs;
import org.apache.hadoop.yarn.submarine.runtimes.common.JobMonitor;
import org.apache.hadoop.yarn.submarine.runtimes.common.JobSubmitter;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES;
import static org.apache.hadoop.yarn.submarine.client.cli.TestRunJobCliParsing.getMockClientContext;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Test class that verifies the correctness of YAML configuration parsing.
 */
public class TestRunJobCliParsingYaml {
  private static final String OVERRIDDEN_PREFIX = "overridden_";
  private File yamlConfig;

  @Before
  public void before() {
    SubmarineLogs.verboseOff();
  }
  
  @After
  public void after() {
    YamlConfigTestUtils.deleteFile(yamlConfig);
  }

  @BeforeClass
  public static void configureResourceTypes() {
    Map<String, ResourceInformation> riMap = initializeMandatoryResources();
    ResourceInformation res1 = ResourceInformation.newInstance(ResourceInformation.GPU_URI,
        ResourceInformation.VCORES.getUnits(), 0, 4);
    riMap.put(ResourceInformation.GPU_URI, res1);

    Resources.refreshResourcesFromMap(riMap);
  }

  @Rule
  public ExpectedException exception = ExpectedException.none();

  private static Map<String, ResourceInformation> initializeMandatoryResources() {
    Map<String, ResourceInformation> riMap = new HashMap<>();

    ResourceInformation memory = ResourceInformation.newInstance(
        ResourceInformation.MEMORY_MB.getName(),
        ResourceInformation.MEMORY_MB.getUnits(),
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
        DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);
    ResourceInformation vcores = ResourceInformation.newInstance(
        ResourceInformation.VCORES.getName(),
        ResourceInformation.VCORES.getUnits(),
        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
        DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES);

    riMap.put(ResourceInformation.MEMORY_URI, memory);
    riMap.put(ResourceInformation.VCORES_URI, vcores);
    return riMap;
  }

  private void verifyBasicConfigValues(RunJobParameters jobRunParameters) {
    assertEquals("testInputPath", jobRunParameters.getInputPath());
    assertEquals("testCheckpointPath", jobRunParameters.getCheckpointPath());
    assertEquals("testDockerImage", jobRunParameters.getDockerImageName());

    assertNotNull(jobRunParameters.getLocalizations());
    assertEquals(2, jobRunParameters.getLocalizations().size());

    assertNotNull(jobRunParameters.getQuicklinks());
    assertEquals(2, jobRunParameters.getQuicklinks().size());

    assertTrue(SubmarineLogs.isVerbose());
    assertTrue(jobRunParameters.isWaitJobFinish());
  }

  private void verifyPsValues(RunJobParameters jobRunParameters, String prefix) {
    assertEquals(4, jobRunParameters.getNumPS());
    assertEquals(prefix + "testLaunchCmdPs", jobRunParameters.getPSLaunchCmd());
    assertEquals(prefix + "testDockerImagePs", jobRunParameters.getPsDockerImage());
    assertEquals(ResourceTypesTestHelper.newResource(20500L, 34,
        ImmutableMap.<String, String> builder()
            .put(ResourceInformation.GPU_URI, "4").build()),
        jobRunParameters.getPsResource());
  }

  private void verifyWorkerValues(RunJobParameters jobRunParameters, String prefix) {
    assertEquals(3, jobRunParameters.getNumWorkers());
    assertEquals(prefix + "testLaunchCmdWorker", jobRunParameters.getWorkerLaunchCmd());
    assertEquals(prefix + "testDockerImageWorker", jobRunParameters.getWorkerDockerImage());
    assertEquals(ResourceTypesTestHelper.newResource(20480L, 32,
        ImmutableMap.<String, String> builder()
            .put(ResourceInformation.GPU_URI, "2").build()),
        jobRunParameters.getWorkerResource());
  }

  private void verifySecurityValues(RunJobParameters jobRunParameters) {
    assertEquals("keytabPath", jobRunParameters.getKeytab());
    assertEquals("testPrincipal", jobRunParameters.getPrincipal());
    assertTrue(jobRunParameters.isDistributeKeytab());
  }

  private void verifyTensorboardValues(RunJobParameters jobRunParameters) {
    assertTrue(jobRunParameters.isTensorboardEnabled());
    assertEquals("tensorboardDockerImage", jobRunParameters.getTensorboardDockerImage());
    assertEquals(ResourceTypesTestHelper.newResource(21000L, 37,
        ImmutableMap.<String, String> builder()
            .put(ResourceInformation.GPU_URI, "3").build()),
        jobRunParameters.getTensorboardResource());
  }

  @Test
  public void testValidYamlParsing() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());
    Assert.assertFalse(SubmarineLogs.isVerbose());

    yamlConfig = YamlConfigTestUtils.createTempFileWithContents(
        "runjobcliparsing/valid-config.yaml");
    runJobCli.run(
        new String[] { "--spec", yamlConfig.getAbsolutePath(), "--verbose" });

    RunJobParameters jobRunParameters = runJobCli.getRunJobParameters();
    verifyBasicConfigValues(jobRunParameters);
    verifyPsValues(jobRunParameters, "");
    verifyWorkerValues(jobRunParameters, "");
    verifySecurityValues(jobRunParameters);
    verifyTensorboardValues(jobRunParameters);
  }

  @Test
  public void testYamlTakesPrecendenceForAllConfigs() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());
    Assert.assertFalse(SubmarineLogs.isVerbose());

    yamlConfig = YamlConfigTestUtils.createTempFileWithContents(
        "runjobcliparsing/valid-config.yaml");
    String[] args = new String[] { "--name", "my-job", "--docker_image", "tf-docker:1.1.0",
        "--input_path", "hdfs://input", "--checkpoint_path", "hdfs://output",
        "--num_workers", "1", "--worker_launch_cmd", "python run-job.py",
        "--worker_resources", "memory=4g,vcores=2", "--tensorboard",
        "true", "--verbose", "--wait_job_finish", "--spec", yamlConfig.getAbsolutePath() };
    runJobCli.run(args);

    RunJobParameters jobRunParameters = runJobCli.getRunJobParameters();
    verifyBasicConfigValues(jobRunParameters);
    verifyPsValues(jobRunParameters, "");
    verifyWorkerValues(jobRunParameters, "");
    verifySecurityValues(jobRunParameters);
    verifyTensorboardValues(jobRunParameters);
  }

  @Test
  public void testRoleOverrides() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());
    Assert.assertFalse(SubmarineLogs.isVerbose());

    yamlConfig = YamlConfigTestUtils.createTempFileWithContents(
        "runjobcliparsing/valid-config-with-overrides.yaml");
    runJobCli.run(
        new String[]{"--spec", yamlConfig.getAbsolutePath(), "--verbose"});

    RunJobParameters jobRunParameters = runJobCli.getRunJobParameters();
    verifyBasicConfigValues(jobRunParameters);
    verifyPsValues(jobRunParameters, OVERRIDDEN_PREFIX);
    verifyWorkerValues(jobRunParameters, OVERRIDDEN_PREFIX);
    verifySecurityValues(jobRunParameters);
    verifyTensorboardValues(jobRunParameters);
  }

  @Test
  public void testFalseValuesForBooleanFields() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());
    Assert.assertFalse(SubmarineLogs.isVerbose());

    yamlConfig = YamlConfigTestUtils.createTempFileWithContents(
        "runjobcliparsing/test-false-values.yaml");
    runJobCli.run(
        new String[] { "--spec", yamlConfig.getAbsolutePath(), "--verbose" });
    RunJobParameters jobRunParameters = runJobCli.getRunJobParameters();

    assertFalse(jobRunParameters.isDistributeKeytab());
    assertFalse(jobRunParameters.isWaitJobFinish());
    assertFalse(jobRunParameters.isTensorboardEnabled());
  }

  @Test
  public void testWrongIndentation() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());
    Assert.assertFalse(SubmarineLogs.isVerbose());

    yamlConfig = YamlConfigTestUtils.createTempFileWithContents(
        "runjobcliparsing/wrong-indentation.yaml");

    exception.expect(YamlParseException.class);
    exception.expectMessage("Failed to parse YAML file " + yamlConfig.getAbsolutePath());
    runJobCli.run(
        new String[]{"--spec", yamlConfig.getAbsolutePath(), "--verbose"});
  }

  @Test
  public void testWrongFilename() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());
    Assert.assertFalse(SubmarineLogs.isVerbose());

    exception.expect(YamlParseException.class);
    runJobCli.run(
        new String[]{"--spec", "not-existing", "--verbose"});
  }

  @Test
  public void testEmptyFile() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());

    yamlConfig = YamlConfigTestUtils.createEmptyTempFile();

    exception.expect(YamlParseException.class);
    runJobCli.run(
        new String[]{"--spec", yamlConfig.getAbsolutePath(), "--verbose"});
  }

  @Test
  public void testWrongPropertyName() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());

    yamlConfig = YamlConfigTestUtils.createTempFileWithContents("runjobcliparsing/wrong-property-name.yaml");

    exception.expect(YamlParseException.class);
    exception.expectMessage("Failed to parse YAML file " + yamlConfig.getAbsolutePath());
    runJobCli.run(
        new String[]{"--spec", yamlConfig.getAbsolutePath(), "--verbose"});
  }

  @Test
  public void testMissingConfigsSection() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());

    yamlConfig = YamlConfigTestUtils.createTempFileWithContents("runjobcliparsing/missing-configs.yaml");

    exception.expect(YamlParseException.class);
    exception.expectMessage("Config section should be defined, but it cannot be found");
    runJobCli.run(
        new String[]{"--spec", yamlConfig.getAbsolutePath(), "--verbose"});
  }

  @Test
  public void testMissingSectionsShouldParsed() throws Exception {
    RunJobCli runJobCli = new RunJobCli(getMockClientContext());

    yamlConfig = YamlConfigTestUtils.createTempFileWithContents("runjobcliparsing/some-sections-missing.yaml");
    runJobCli.run(
        new String[]{"--spec", yamlConfig.getAbsolutePath(), "--verbose"});
  }
}
