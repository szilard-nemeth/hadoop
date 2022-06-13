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

package org.apache.hadoop.yarn.logaggregation;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.logaggregation.filecontroller.LogAggregationFileController;
import org.apache.hadoop.yarn.logaggregation.filecontroller.ifile.LogAggregationIndexedFileController;
import org.apache.hadoop.yarn.logaggregation.filecontroller.tfile.LogAggregationTFileController;
import org.apache.hadoop.yarn.logaggregation.testutils.AggregatedLogDeletionServiceForTest;
import org.apache.hadoop.yarn.logaggregation.testutils.LogAggregationFilesBuilder;
import org.apache.hadoop.yarn.logaggregation.testutils.PathWithFileStatus;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.LOG_AGGREGATION_FILE_CONTROLLER_FMT;
import static org.apache.hadoop.yarn.logaggregation.LogAggregationTestUtils.enableFileControllers;
import static org.apache.hadoop.yarn.logaggregation.testutils.FileStatusUtils.*;
import static org.apache.hadoop.yarn.logaggregation.testutils.MockRMClientUtils.createMockRMClient;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class TestAggregatedLogDeletionService {
  static final Logger LOG = LoggerFactory.getLogger(TestAggregatedLogDeletionService.class);
  private static final String T_FILE = "TFile";
  private static final String I_FILE = "IFile";
  private static final String USER_ME = "me";
  private static final String DIR_HOST1 = "host1";
  private static final String DIR_HOST2 = "host2";

  private static final String ROOT = "mockfs://foo/";
  private static final String REMOTE_ROOT_LOG_DIR = ROOT + "tmp/logs/";
  private static final String SUFFIX = "logs";
  private static final String NEW_SUFFIX = LogAggregationUtils.getBucketSuffix() + SUFFIX;
  private static final int TEN_DAYS_IN_SECONDS = 10 * 24 * 3600;

  private static final List<Class<? extends LogAggregationFileController>>
          ALL_FILE_CONTROLLERS = Arrays.asList(
          LogAggregationIndexedFileController.class,
          LogAggregationTFileController.class);
  public static final List<String> ALL_FILE_CONTROLLER_NAMES = Arrays.asList(I_FILE, T_FILE);

  @BeforeClass
  public static void beforeClass() {
    org.apache.log4j.Logger.getRootLogger().setLevel(Level.DEBUG);
  }

  @Before
  public void closeFilesystems() throws IOException {
    // prevent the same mockfs instance from being reused due to FS cache
    FileSystem.closeAll();
  }

  private Configuration setupConfiguration(int retainSeconds, int retainCheckIntervalSeconds) {
    Configuration conf = new Configuration();
    conf.setClass("fs.mockfs.impl", MockFileSystem.class, FileSystem.class);
    conf.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, true);
    conf.setInt(YarnConfiguration.LOG_AGGREGATION_RETAIN_SECONDS, retainSeconds);
    conf.setInt(YarnConfiguration.LOG_AGGREGATION_RETAIN_CHECK_INTERVAL_SECONDS,
            retainCheckIntervalSeconds);
    conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR, REMOTE_ROOT_LOG_DIR);
    conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR_SUFFIX, SUFFIX);
    conf.set(YarnConfiguration.LOG_AGGREGATION_FILE_FORMATS, T_FILE);
    conf.set(String.format(LOG_AGGREGATION_FILE_CONTROLLER_FMT, T_FILE),
            LogAggregationTFileController.class.getName());
    return conf;
  }

  @Test
  public void testDeletion() throws Exception {
    long now = System.currentTimeMillis();
    long toDeleteTime = now - (2000 * 1000);
    long toKeepTime = now - (1500 * 1000);

    Configuration conf = setupConfiguration(1800, -1);
    long timeout = 2000L;
    LogAggregationFilesBuilder.create(conf)
            .withRootPath(ROOT)
            .withRemoteRootLogPath(REMOTE_ROOT_LOG_DIR)
            .withUserDir(USER_ME, toKeepTime)
            .withSuffixDir(SUFFIX, toDeleteTime)
            .withBucketDir(toDeleteTime)
            .withApps(new LogAggregationFilesBuilder.AppDescriptor(toDeleteTime, new Pair[] {}),
                    new LogAggregationFilesBuilder.AppDescriptor(toDeleteTime,
                            Pair.of(DIR_HOST1, toDeleteTime),
                            Pair.of(DIR_HOST2, toKeepTime)),
                    new LogAggregationFilesBuilder.AppDescriptor(toDeleteTime,
                            Pair.of(DIR_HOST1, toDeleteTime),
                            Pair.of(DIR_HOST2, toDeleteTime)),
                    new LogAggregationFilesBuilder.AppDescriptor(toDeleteTime,
                            Pair.of(DIR_HOST1, toDeleteTime),
                            Pair.of(DIR_HOST2, toKeepTime)))
            .withFinishedApps(1, 2, 3)
            .withRunningApps(4)
            .injectExceptionForAppDirDeletion(3)
            .setupMocks()
            .setupAndRunDeletionService()
            .verifyAppDirDeleted(1, timeout)
            .verifyAppDirDeleted(3, timeout)
            .verifyAppDirNotDeleted(2, timeout)
            .verifyAppDirNotDeleted(4, timeout)
            .verifyAppFileDeleted(4, 1, timeout)
            .verifyAppFileNotDeleted(4, 2, timeout)
            .teardown();
  }
  
  @Test
  public void testDeletionTwoControllers() throws IOException {
    long now = System.currentTimeMillis();
    long toDeleteTime = now - (2000 * 1000);
    long toKeepTime = now - (1500 * 1000);

    Configuration conf = setupConfiguration(1800, -1);
    enableFileControllers(conf, REMOTE_ROOT_LOG_DIR, ALL_FILE_CONTROLLERS, ALL_FILE_CONTROLLER_NAMES);
    long timeout = 2000L;
    LogAggregationFilesBuilder.create(conf)
            .withRootPath(ROOT)
            .withRemoteRootLogPath(REMOTE_ROOT_LOG_DIR)
            .withBothFileControllers()
            .withUserDir(USER_ME, toKeepTime)
            .withSuffixDir(NEW_SUFFIX, toDeleteTime)
            .withBucketDir(toDeleteTime)
            .withApps(//Apps for TFile
                    new LogAggregationFilesBuilder.AppDescriptor(T_FILE, toDeleteTime, new Pair[]{}),
                    new LogAggregationFilesBuilder.AppDescriptor(T_FILE, toDeleteTime,
                            Pair.of(DIR_HOST1, toDeleteTime),
                            Pair.of(DIR_HOST2, toKeepTime)),
                    new LogAggregationFilesBuilder.AppDescriptor(T_FILE, toDeleteTime,
                            Pair.of(DIR_HOST1, toDeleteTime),
                            Pair.of(DIR_HOST2, toDeleteTime)),
                    new LogAggregationFilesBuilder.AppDescriptor(T_FILE, toDeleteTime,
                            Pair.of(DIR_HOST1, toDeleteTime),
                            Pair.of(DIR_HOST2, toKeepTime)),
                    //Apps for IFile
                    new LogAggregationFilesBuilder.AppDescriptor(I_FILE, toDeleteTime, new Pair[]{}),
                    new LogAggregationFilesBuilder.AppDescriptor(I_FILE, toDeleteTime,
                            Pair.of(DIR_HOST1, toDeleteTime),
                            Pair.of(DIR_HOST2, toKeepTime)),
                    new LogAggregationFilesBuilder.AppDescriptor(I_FILE, toDeleteTime,
                            Pair.of(DIR_HOST1, toDeleteTime),
                            Pair.of(DIR_HOST2, toDeleteTime)),
                    new LogAggregationFilesBuilder.AppDescriptor(I_FILE, toDeleteTime,
                            Pair.of(DIR_HOST1, toDeleteTime),
                            Pair.of(DIR_HOST2, toKeepTime)))
            .withFinishedApps(1, 2, 3, 5, 6, 7)
            .withRunningApps(4, 8)
            .injectExceptionForAppDirDeletion(3, 6)
            .setupMocks()
            .setupAndRunDeletionService()
            .verifyAppDirsDeleted(timeout, 1, 3, 5, 7)
            .verifyAppDirsNotDeleted(timeout, 2, 4, 6, 8)
            .verifyAppFilesDeleted(timeout, new Pair[] {Pair.of(4, 1), Pair.of(8, 1)})
            .verifyAppFilesNotDeleted(timeout, new Pair[] {Pair.of(4, 2), Pair.of(8, 2)})
            .teardown();
  }

  @Test
  public void testRefreshLogRetentionSettings() throws Exception {
    long now = System.currentTimeMillis();
    long before2000Secs = now - (2000 * 1000);
    long before50Secs = now - (50 * 1000);
    int checkIntervalSeconds = 2;
    int checkIntervalMilliSeconds = checkIntervalSeconds * 1000;

    Configuration conf = setupConfiguration(1800, 1);

    Path rootPath = new Path(ROOT);
    FileSystem rootFs = rootPath.getFileSystem(conf);
    FileSystem mockFs = ((FilterFileSystem) rootFs).getRawFileSystem();

    ApplicationId appId1 = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    ApplicationId appId2 = ApplicationId.newInstance(System.currentTimeMillis(), 2);
    
    Path remoteRootLogPath = new Path(REMOTE_ROOT_LOG_DIR);

    PathWithFileStatus userDir = createDirLogPathWithFileStatus(remoteRootLogPath, USER_ME,
            before50Secs);
    PathWithFileStatus suffixDir = createDirLogPathWithFileStatus(userDir.path, NEW_SUFFIX,
            before50Secs);
    PathWithFileStatus bucketDir = createDirBucketDirLogPathWithFileStatus(remoteRootLogPath,
            USER_ME, SUFFIX, appId1, before50Secs);

    when(mockFs.listStatus(remoteRootLogPath)).thenReturn(new FileStatus[] {userDir.fileStatus});

    //Set time last modified of app1Dir directory and its files to before2000Secs 
    PathWithFileStatus app1 = createPathWithFileStatusForAppId(remoteRootLogPath, appId1,
            USER_ME, SUFFIX, before2000Secs);

    //Set time last modified of app1Dir directory and its files to before50Secs 
    PathWithFileStatus app2 = createPathWithFileStatusForAppId(remoteRootLogPath, appId2,
            USER_ME, SUFFIX, before50Secs);

    when(mockFs.listStatus(userDir.path)).thenReturn(new FileStatus[]{suffixDir.fileStatus});
    when(mockFs.listStatus(suffixDir.path)).thenReturn(new FileStatus[]{bucketDir.fileStatus});
    when(mockFs.listStatus(bucketDir.path)).thenReturn(new FileStatus[]{app1.fileStatus,
            app2.fileStatus});

    PathWithFileStatus app1Log1 = createFileLogPathWithFileStatus(app1.path, DIR_HOST1,
            before2000Secs);
    PathWithFileStatus app2Log1 = createFileLogPathWithFileStatus(app2.path, DIR_HOST1,
            before50Secs);

    when(mockFs.listStatus(app1.path)).thenReturn(new FileStatus[] {app1Log1.fileStatus});
    when(mockFs.listStatus(app2.path)).thenReturn(new FileStatus[] {app2Log1.fileStatus});

    final List<ApplicationId> finishedApplications =
        Collections.unmodifiableList(Arrays.asList(appId1, appId2));

    AggregatedLogDeletionService deletionSvc = new AggregatedLogDeletionServiceForTest(null,
            finishedApplications, conf);
    
    deletionSvc.init(conf);
    deletionSvc.start();
    
    //app1Dir would be deleted since its done above log retention period
    verify(mockFs, timeout(10000)).delete(app1.path, true);
    //app2Dir is not expected to be deleted since it is below the threshold
    verify(mockFs, timeout(3000).times(0)).delete(app2.path, true);

    //Now, let's change the confs
    conf.setInt(YarnConfiguration.LOG_AGGREGATION_RETAIN_SECONDS, 50);
    conf.setInt(YarnConfiguration.LOG_AGGREGATION_RETAIN_CHECK_INTERVAL_SECONDS,
            checkIntervalSeconds);
    //We have not called refreshLogSettings,hence don't expect to see the changed conf values
    assertTrue(checkIntervalMilliSeconds != deletionSvc.getCheckIntervalMsecs());
    
    //refresh the log settings
    deletionSvc.refreshLogRetentionSettings();

    //Check interval time should reflect the new value
    Assert.assertEquals(checkIntervalMilliSeconds, deletionSvc.getCheckIntervalMsecs());
    //app2Dir should be deleted since it falls above the threshold
    verify(mockFs, timeout(10000)).delete(app2.path, true);
    deletionSvc.stop();
  }
  
  @Test
  public void testCheckInterval() throws Exception {
    long now = System.currentTimeMillis();
    long toDeleteTime = now - TEN_DAYS_IN_SECONDS * 1000;

    Configuration conf = setupConfiguration(TEN_DAYS_IN_SECONDS, 1);

    // prevent us from picking up the same mockfs instance from another test
    FileSystem.closeAll();
    Path rootPath = new Path(ROOT);
    FileSystem rootFs = rootPath.getFileSystem(conf);
    FileSystem mockFs = ((FilterFileSystem)rootFs).getRawFileSystem();

    Path remoteRootLogPath = new Path(REMOTE_ROOT_LOG_DIR);

    PathWithFileStatus userDir = createDirLogPathWithFileStatus(remoteRootLogPath, USER_ME, now);
    PathWithFileStatus suffixDir = createDirLogPathWithFileStatus(userDir.path, NEW_SUFFIX, now);

    when(mockFs.listStatus(remoteRootLogPath)).thenReturn(new FileStatus[]{userDir.fileStatus});

    ApplicationId appId1 = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    PathWithFileStatus bucketDir = createDirBucketDirLogPathWithFileStatus(remoteRootLogPath,
            USER_ME, SUFFIX, appId1, now);

    PathWithFileStatus app1 = createPathWithFileStatusForAppId(remoteRootLogPath, appId1,
            USER_ME, SUFFIX, now);
    PathWithFileStatus app1Log1 = createFileLogPathWithFileStatus(app1.path, DIR_HOST1, now);

    when(mockFs.listStatus(userDir.path)).thenReturn(new FileStatus[] {suffixDir.fileStatus});
    when(mockFs.listStatus(suffixDir.path)).thenReturn(new FileStatus[] {bucketDir.fileStatus});
    when(mockFs.listStatus(bucketDir.path)).thenReturn(new FileStatus[] {app1.fileStatus});
    when(mockFs.listStatus(app1.path)).thenReturn(new FileStatus[]{app1Log1.fileStatus});

    final List<ApplicationId> finishedApplications = Collections.singletonList(appId1);

    AggregatedLogDeletionService deletionSvc = new AggregatedLogDeletionServiceForTest(null,
            finishedApplications);
    deletionSvc.init(conf);
    deletionSvc.start();
 
    verify(mockFs, timeout(10000).atLeast(4)).listStatus(any(Path.class));
    verify(mockFs, never()).delete(app1.path, true);

    // modify the timestamp of the logs and verify if it's picked up quickly
    app1.changeModificationTime(toDeleteTime);
    app1Log1.changeModificationTime(toDeleteTime);
    bucketDir.changeModificationTime(toDeleteTime);
    when(mockFs.listStatus(userDir.path)).thenReturn(new FileStatus[] {suffixDir.fileStatus});
    when(mockFs.listStatus(suffixDir.path)).thenReturn(new FileStatus[] {bucketDir.fileStatus });
    when(mockFs.listStatus(bucketDir.path)).thenReturn(new FileStatus[] {app1.fileStatus });
    when(mockFs.listStatus(app1.path)).thenReturn(new FileStatus[]{app1Log1.fileStatus});

    verify(mockFs, timeout(10000)).delete(app1.path, true);

    deletionSvc.stop();
  }

  @Test
  public void testRobustLogDeletion() throws Exception {
    Configuration conf = setupConfiguration(TEN_DAYS_IN_SECONDS, 1);

    // prevent us from picking up the same mockfs instance from another test
    FileSystem.closeAll();
    Path rootPath = new Path(ROOT);
    FileSystem rootFs = rootPath.getFileSystem(conf);
    FileSystem mockFs = ((FilterFileSystem)rootFs).getRawFileSystem();

    Path remoteRootLogPath = new Path(REMOTE_ROOT_LOG_DIR);

    PathWithFileStatus userDir = createDirLogPathWithFileStatus(remoteRootLogPath, USER_ME, 0);
    PathWithFileStatus suffixDir = createDirLogPathWithFileStatus(userDir.path, NEW_SUFFIX, 0);
    PathWithFileStatus bucketDir = createDirLogPathWithFileStatus(suffixDir.path, "0", 0);

    when(mockFs.listStatus(remoteRootLogPath)).thenReturn(new FileStatus[]{userDir.fileStatus});
    when(mockFs.listStatus(userDir.path)).thenReturn(new FileStatus[]{suffixDir.fileStatus});
    when(mockFs.listStatus(suffixDir.path)).thenReturn(new FileStatus[]{bucketDir.fileStatus});

    ApplicationId appId1 = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    ApplicationId appId2 = ApplicationId.newInstance(System.currentTimeMillis(), 2);
    ApplicationId appId3 = ApplicationId.newInstance(System.currentTimeMillis(), 3);

    PathWithFileStatus app1 = createDirLogPathWithFileStatus(bucketDir.path, appId1.toString(), 0);
    PathWithFileStatus app2 = createDirLogPathWithFileStatus(bucketDir.path, "application_a", 0);
    PathWithFileStatus app3 = createDirLogPathWithFileStatus(bucketDir.path, appId3.toString(), 0);
    PathWithFileStatus app3Log3 = createDirLogPathWithFileStatus(app3.path, DIR_HOST1, 0);
    
    when(mockFs.listStatus(bucketDir.path)).thenReturn(new FileStatus[]{
            app1.fileStatus,app2.fileStatus, app3.fileStatus});
    when(mockFs.listStatus(app1.path)).thenThrow(
            new RuntimeException("Should be caught and logged"));
    when(mockFs.listStatus(app2.path)).thenReturn(new FileStatus[]{});
    when(mockFs.listStatus(app3.path)).thenReturn(new FileStatus[]{app3Log3.fileStatus});

    final List<ApplicationId> finishedApplications = Collections.unmodifiableList(
            Arrays.asList(appId1, appId3));

    ApplicationClientProtocol rmClient = createMockRMClient(finishedApplications, null);
    AggregatedLogDeletionService.LogDeletionTask deletionTask =
        new AggregatedLogDeletionService.LogDeletionTask(conf, TEN_DAYS_IN_SECONDS, rmClient);
    deletionTask.run();
    verify(mockFs).delete(app3.path, true);
  }

  static class MockFileSystem extends FilterFileSystem {
    MockFileSystem() {
      super(mock(FileSystem.class));
    }

    public void initialize(URI name, Configuration conf) throws IOException {}

    @Override
    public boolean hasPathCapability(Path path, String capability) throws IOException {
      return true;
    }
  }

}
