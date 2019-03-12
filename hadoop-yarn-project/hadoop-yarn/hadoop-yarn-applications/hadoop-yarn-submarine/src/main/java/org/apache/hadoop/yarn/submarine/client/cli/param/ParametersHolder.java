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

package org.apache.hadoop.yarn.submarine.client.cli.param;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.yarn.submarine.client.cli.Cli;
import org.apache.hadoop.yarn.submarine.client.cli.CliConstants;
import org.apache.hadoop.yarn.submarine.client.cli.param.yaml.Configs;
import org.apache.hadoop.yarn.submarine.client.cli.param.yaml.Role;
import org.apache.hadoop.yarn.submarine.client.cli.param.yaml.Roles;
import org.apache.hadoop.yarn.submarine.client.cli.param.yaml.Scheduling;
import org.apache.hadoop.yarn.submarine.client.cli.param.yaml.Security;
import org.apache.hadoop.yarn.submarine.client.cli.param.yaml.TensorBoard;
import org.apache.hadoop.yarn.submarine.client.cli.param.yaml.YamlConfigFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This class acts as a wrapper of {@code CommandLine} values along with YAML configuration values.
 * YAML configuration is only stored if a spec file is specified within the CLI arguments.
 * Using this wrapper class makes easy to deal with
 * any form of configuration source potentially added into Submarine, in the future.
 * Currently, YAML configuration values take precedence over CLI arguments.
 */
public class ParametersHolder {
  private static final Logger LOG =
      LoggerFactory.getLogger(ParametersHolder.class);
  
  private final CommandLine parsedCommandLine;
  private final Map<String, String> yamlStringConfigs;
  private final Map<String, List<String>> yamlListConfigs;
  private final boolean useYamlConfig;
  private final ImmutableSet onlyDefinedWithCliArgs = ImmutableSet.of(CliConstants.VERBOSE);
  

  private ParametersHolder(CommandLine parsedCommandLine, YamlConfigFile yamlConfig) {
    this.parsedCommandLine = parsedCommandLine;
    this.useYamlConfig = yamlConfig != null;
    if (this.useYamlConfig) {
      this.yamlStringConfigs = initStringConfigValues(yamlConfig);
      this.yamlListConfigs = initListConfigValues(yamlConfig);
    } else {
      this.yamlStringConfigs = Collections.emptyMap();
      this.yamlListConfigs = Collections.emptyMap();
    }
  }

  /**
   * Maps every value coming from the passed yamlConfig to {@code CliConstants}.
   * @param yamlConfig Parsed YAML config
   * @return A map of config values, keys are {@code CliConstants} and values are Strings.
   */
  private Map<String, String> initStringConfigValues(YamlConfigFile yamlConfig) {
    Map<String, String> yamlConfigValues = Maps.newHashMap();
    Roles roles = yamlConfig.getRoles();
    
    initGenericConfigs(yamlConfig, yamlConfigValues, yamlConfig.getConfigs());
    initPs(yamlConfigValues, roles.getPs());
    initWorker(yamlConfigValues, roles.getWorker());
    initScheduling(yamlConfigValues, yamlConfig.getScheduling());
    initSecurity(yamlConfigValues, yamlConfig.getSecurity());
    initTensorBoard(yamlConfigValues, yamlConfig.getTensorBoard());
    
    return yamlConfigValues;
  }

  private void initGenericConfigs(YamlConfigFile yamlConfig, Map<String, String> yamlConfigValues, Configs configs) {
    yamlConfigValues.put(CliConstants.NAME, yamlConfig.getSpec().getName());
    yamlConfigValues.put(CliConstants.INPUT_PATH, configs.getInputPath());
    yamlConfigValues.put(CliConstants.CHECKPOINT_PATH, configs.getCheckpointPath());
    yamlConfigValues.put(CliConstants.SAVED_MODEL_PATH, configs.getSavedModelPath());
    yamlConfigValues.put(CliConstants.DOCKER_IMAGE, configs.getDockerImage());
    yamlConfigValues.put(CliConstants.WAIT_JOB_FINISH, configs.getWaitJobFinish());
  }

  private void initPs(Map<String, String> yamlConfigValues, Role ps) {
    if (ps == null) {
      return;
    }
    yamlConfigValues.put(CliConstants.N_PS, String.valueOf(ps.getReplicas()));
    yamlConfigValues.put(CliConstants.PS_RES, ps.getResources());
    yamlConfigValues.put(CliConstants.PS_DOCKER_IMAGE, ps.getDockerImage());
    yamlConfigValues.put(CliConstants.PS_LAUNCH_CMD, ps.getLaunchCmd());
  }

  private void initWorker(Map<String, String> yamlConfigValues, Role worker) {
    if (worker == null) {
      return;
    }
    yamlConfigValues.put(CliConstants.N_WORKERS, String.valueOf(worker.getReplicas()));
    yamlConfigValues.put(CliConstants.WORKER_RES, worker.getResources());
    yamlConfigValues.put(CliConstants.WORKER_DOCKER_IMAGE, worker.getDockerImage());
    yamlConfigValues.put(CliConstants.WORKER_LAUNCH_CMD, worker.getLaunchCmd());
  }

  private void initScheduling(Map<String, String> yamlConfigValues,
      Scheduling scheduling) {
    if (scheduling == null) {
      return;
    }
    yamlConfigValues.put(CliConstants.QUEUE, scheduling.getQueue());
  }

  private void initSecurity(Map<String, String> yamlConfigValues, Security security) {
    if (security == null) {
      return;
    }
    yamlConfigValues.put(CliConstants.KEYTAB, security.getKeytab());
    yamlConfigValues.put(CliConstants.PRINCIPAL, security.getPrincipal());
    yamlConfigValues.put(CliConstants.DISTRIBUTE_KEYTAB,
        String.valueOf(security.isDistributeKeytab()));
  }

  private void initTensorBoard(Map<String, String> yamlConfigValues,
      TensorBoard tensorBoard) {
    if (tensorBoard == null) {
      return;
    }
    yamlConfigValues.put(CliConstants.TENSORBOARD, Boolean.TRUE.toString());
    yamlConfigValues.put(CliConstants.TENSORBOARD_DOCKER_IMAGE,
        tensorBoard.getDockerImage());
    yamlConfigValues.put(CliConstants.TENSORBOARD_RESOURCES,
        tensorBoard.getResources());
  }

  private Map<String, List<String>> initListConfigValues(YamlConfigFile yamlConfig) {
    Map<String, List<String>> yamlConfigValues = Maps.newHashMap();
    Configs configs = yamlConfig.getConfigs();
    yamlConfigValues.put(CliConstants.LOCALIZATION, configs.getLocalizations());
    yamlConfigValues.put(CliConstants.ENV,
        convertToEnvsList(configs.getEnvs()));
    yamlConfigValues.put(CliConstants.QUICKLINK, configs.getQuicklinks());

    return yamlConfigValues;
  }

  private List<String> convertToEnvsList(Map<String, String> envs) {
    return envs.entrySet().stream()
        .map(e -> String.format("%s=%s", e.getKey(), e.getValue()))
        .collect(Collectors.toList());
  }

  public static ParametersHolder createWithCmdLine(CommandLine cli) {
    return new ParametersHolder(cli, null);
  }

  public static ParametersHolder createWithCmdLineAndYaml(CommandLine cli,
      YamlConfigFile yamlConfig) {
    return new ParametersHolder(cli, yamlConfig);
  }

  /**
   * Gets the option value, either from the CLI arguments or YAML config, if present.
   * Values of YAML config (if YAML is present) take precedence over CLI values. 
   * @param option Name of the config.
   * @return The value of the config
   */
  String getOptionValue(String option) {
    if (useYamlConfig && !onlyDefinedWithCliArgs.contains(option)) {
      LOG.debug("Querying config value for key: {}" + " from YAML configuration", option);
      String value = yamlStringConfigs.get(option);
      LOG.debug("Found config value {} for key {} from YAML configuration.", value, option);
      return value;
    }
    LOG.debug("Querying config value for key: {}" + " from CLI configuration", option);
    String value = parsedCommandLine.getOptionValue(option);
    LOG.debug("Found config value {} for key {} from CLI configuration.", value, option);
    return value;
  }

  /**
   * Gets the option values, either from the CLI arguments or YAML config, if present.
   * Values of YAML config (if YAML is present) take precedence over CLI values. 
   * @param option Name of the config.
   * @return The values of the config
   */
  List<String> getOptionValues(String option) {
    if (useYamlConfig && !onlyDefinedWithCliArgs.contains(option)) {
      LOG.debug("Querying config value for key: {}" + " from YAML configuration", option);
      List<String> values = yamlListConfigs.get(option);
      LOG.debug("Found config values {} for key {} from YAML configuration.", values, option);
      return values;
    }
    LOG.debug("Querying config value for key: {}" + " from CLI configuration", option);
    String[] optionValues = parsedCommandLine.getOptionValues(option);
    if (optionValues != null) {
      List<String> values = Arrays.asList(optionValues);
      LOG.debug("Found config values {} for key {} from CLI configuration.", values, option);
      return values;
    } else {
      LOG.debug("No config values are found for key {} from CLI configuration.", option);
      return Lists.newArrayList();
    }
  }

  boolean hasOption(String option) {
    if (useYamlConfig && !onlyDefinedWithCliArgs.contains(option)) {
      String stringValue = yamlStringConfigs.get(option);
      return stringValue != null && Boolean.valueOf(stringValue).equals(Boolean.TRUE);
    }
    return parsedCommandLine.hasOption(option);
  }
}
