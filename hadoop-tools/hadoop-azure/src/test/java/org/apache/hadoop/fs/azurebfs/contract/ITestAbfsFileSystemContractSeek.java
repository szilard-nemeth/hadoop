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

package org.apache.hadoop.fs.azurebfs.contract;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStream;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStreamStatisticsImpl;
import org.apache.hadoop.fs.contract.AbstractContractSeekTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_READ_AHEAD_RANGE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_READ_BUFFER_SIZE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.MIN_BUFFER_SIZE;
import static org.apache.hadoop.fs.contract.ContractTestUtils.createFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.impl.FutureIOSupport.awaitFuture;

/**
 * Contract test for seek operation.
 */
public class ITestAbfsFileSystemContractSeek extends AbstractContractSeekTest{
  private final boolean isSecure;
  private final ABFSContractTestBinding binding;

  private static final byte[] BLOCK = dataset(100 * 1024, 0, 255);

  public ITestAbfsFileSystemContractSeek() throws Exception {
    binding = new ABFSContractTestBinding();
    this.isSecure = binding.isSecureMode();
  }

  @Override
  public void setup() throws Exception {
    binding.setup();
    super.setup();
  }

  @Override
  protected Configuration createConfiguration() {
    return binding.getRawConfiguration();
  }

  @Override
  protected AbstractFSContract createContract(final Configuration conf) {
    conf.setInt(AZURE_READ_AHEAD_RANGE, MIN_BUFFER_SIZE);
    conf.setInt(AZURE_READ_BUFFER_SIZE, MIN_BUFFER_SIZE);
    return new AbfsFileSystemContract(conf, isSecure);
  }

  /**
   * Test verifies if the data is read correctly
   * when {@code ConfigurationKeys#AZURE_READ_AHEAD_RANGE} is set.
   */
  @Test
  public void testSeekAndReadWithReadAhead() throws IOException {
    describe(" Testing seek and read with read ahead " +
            "enabled for random reads");

    Path testSeekFile = path(getMethodName() + "bigseekfile.txt");
    createDataSet(testSeekFile);
    try (FSDataInputStream in = getFileSystem().open(testSeekFile)) {
      AbfsInputStream inStream = ((AbfsInputStream) in.getWrappedStream());
      AbfsInputStreamStatisticsImpl streamStatistics = inStream.getStreamStatistics();
      assertEquals(String.format("Value of %s is not set correctly", AZURE_READ_AHEAD_RANGE),
              MIN_BUFFER_SIZE, inStream.getReadAheadRange());

      long remoteReadOperationsOldVal = streamStatistics.remoteReadOperations;
      assertEquals("Number of remote read ops should be 0 " +
              "before any read call is made", 0, remoteReadOperationsOldVal);

      // Test read at first position. Remote read.
      assertEquals("First call to getPos() should return 0",
              0, inStream.getPos());
      assertDataAtPos(0,  (byte) in.read());
      assertSeekBufferStats(0, streamStatistics.seekInBuffer);
      long remoteReadOperationsNewVal = streamStatistics.remoteReadOperations;
      assertIncrementInRemoteReadOps(remoteReadOperationsOldVal,
              remoteReadOperationsNewVal);
      remoteReadOperationsOldVal = remoteReadOperationsNewVal;

      // Seeking just before read ahead range. Read from buffer.
      int newSeek = inStream.getReadAheadRange() - 1;
      in.seek(newSeek);
      assertGetPosition(newSeek, in.getPos());
      assertDataAtPos(newSeek, (byte) in.read());
      assertSeekBufferStats(1, streamStatistics.seekInBuffer);
      remoteReadOperationsNewVal = streamStatistics.remoteReadOperations;
      assertNoIncrementInRemoteReadOps(remoteReadOperationsOldVal,
              remoteReadOperationsNewVal);
      remoteReadOperationsOldVal = remoteReadOperationsNewVal;

      // Seeking boundary of read ahead range. Read from buffer manager.
      newSeek = inStream.getReadAheadRange();
      inStream.seek(newSeek);
      assertGetPosition(newSeek, in.getPos());
      assertDataAtPos(newSeek, (byte) in.read());
      assertSeekBufferStats(1, streamStatistics.seekInBuffer);
      remoteReadOperationsNewVal = streamStatistics.remoteReadOperations;
      assertNoIncrementInRemoteReadOps(remoteReadOperationsOldVal,
              remoteReadOperationsNewVal);
      remoteReadOperationsOldVal = remoteReadOperationsNewVal;

      // Seeking just after read ahead range. Read from buffer.
      newSeek = inStream.getReadAheadRange() + 1;
      in.seek(newSeek);
      assertGetPosition(newSeek, in.getPos());
      assertDataAtPos(newSeek, (byte) in.read());
      assertSeekBufferStats(2, streamStatistics.seekInBuffer);
      remoteReadOperationsNewVal = streamStatistics.remoteReadOperations;
      assertNoIncrementInRemoteReadOps(remoteReadOperationsOldVal,
              remoteReadOperationsNewVal);
      remoteReadOperationsOldVal = remoteReadOperationsNewVal;

      // Seeking just 10 more bytes such that data is read from buffer.
      newSeek += 10;
      in.seek(newSeek);
      assertGetPosition(newSeek, in.getPos());
      assertDataAtPos(newSeek, (byte) in.read());
      assertSeekBufferStats(3, streamStatistics.seekInBuffer);
      remoteReadOperationsNewVal = streamStatistics.remoteReadOperations;
      assertNoIncrementInRemoteReadOps(remoteReadOperationsOldVal,
              remoteReadOperationsNewVal);
      remoteReadOperationsOldVal = remoteReadOperationsNewVal;

      // Seek backward such that data is read from remote.
      newSeek -= 100;
      in.seek(newSeek);
      assertGetPosition(newSeek, in.getPos());
      assertDataAtPos(newSeek, (byte) in.read());
      assertSeekBufferStats(3, streamStatistics.seekInBuffer);
      remoteReadOperationsNewVal = streamStatistics.remoteReadOperations;
      assertIncrementInRemoteReadOps(remoteReadOperationsOldVal,
              remoteReadOperationsNewVal);
      remoteReadOperationsOldVal = remoteReadOperationsNewVal;

      // Seeking just 10 more bytes such that data is read from buffer.
      newSeek += 10;
      in.seek(newSeek);
      assertGetPosition(newSeek, in.getPos());
      assertDataAtPos(newSeek, (byte) in.read());
      assertSeekBufferStats(4, streamStatistics.seekInBuffer);
      remoteReadOperationsNewVal = streamStatistics.remoteReadOperations;
      assertNoIncrementInRemoteReadOps(remoteReadOperationsOldVal,
              remoteReadOperationsNewVal);
      remoteReadOperationsOldVal = remoteReadOperationsNewVal;

      // Read multiple bytes across read ahead range. Remote read.
      long oldSeek = newSeek;
      newSeek = 2*inStream.getReadAheadRange() -1;
      byte[] bytes = new byte[5];
      in.readFully(newSeek, bytes);
      // With readFully getPos should return oldSeek pos.
      // Adding one as one byte is already read
      // after the last seek is done.
      assertGetPosition(oldSeek + 1, in.getPos());
      assertSeekBufferStats(4, streamStatistics.seekInBuffer);
      assertDatasetEquals(newSeek, "Read across read ahead ",
              bytes, bytes.length);
      remoteReadOperationsNewVal = streamStatistics.remoteReadOperations;
      assertIncrementInRemoteReadOps(remoteReadOperationsOldVal,
              remoteReadOperationsNewVal);
    }
  }

  /**
   * Test to validate the getPos() when a seek is done
   * post {@code AbfsInputStream#unbuffer} call is made.
   * Also using optimised builder api to open file.
   */
  @Test
  public void testSeekAfterUnbuffer() throws IOException {
    describe("Test to make sure that seeking in AbfsInputStream after " +
            "unbuffer() call is not doing anyIO.");
    Path testFile = path(getMethodName() + ".txt");
    createDataSet(testFile);
    final CompletableFuture<FSDataInputStream> future =
            getFileSystem().openFile(testFile)
                    .build();
    try (FSDataInputStream inputStream = awaitFuture(future)) {
      AbfsInputStream abfsInputStream = (AbfsInputStream) inputStream.getWrappedStream();
      AbfsInputStreamStatisticsImpl streamStatistics = abfsInputStream.getStreamStatistics();
      int readAheadRange = abfsInputStream.getReadAheadRange();
      long seekPos = readAheadRange;
      inputStream.seek(seekPos);
      assertDataAtPos(readAheadRange, (byte) inputStream.read());
      long currentRemoteReadOps = streamStatistics.remoteReadOperations;
      assertIncrementInRemoteReadOps(0, currentRemoteReadOps);
      inputStream.unbuffer();
      seekPos -= 10;
      inputStream.seek(seekPos);
      // Seek backwards shouldn't do any IO
      assertNoIncrementInRemoteReadOps(currentRemoteReadOps, streamStatistics.remoteReadOperations);
      assertGetPosition(seekPos, inputStream.getPos());
    }
  }

  private void createDataSet(Path path) throws IOException {
    createFile(getFileSystem(), path, true, BLOCK);
  }

  private void assertGetPosition(long expected, long actual) {
    final String seekPosErrorMsg = "getPos() should return %s";
    assertEquals(String.format(seekPosErrorMsg, expected), expected, actual);
  }

  private void assertDataAtPos(int pos, byte actualData) {
    final String dataErrorMsg = "Mismatch in data@%s";
    assertEquals(String.format(dataErrorMsg, pos), BLOCK[pos], actualData);
  }

  private void assertSeekBufferStats(long expected, long actual) {
    final String statsErrorMsg = "Mismatch in seekInBuffer counts";
    assertEquals(statsErrorMsg, expected, actual);
  }

  private void assertNoIncrementInRemoteReadOps(long oldVal, long newVal) {
    final String incrementErrorMsg = "Number of remote read ops shouldn't increase";
    assertEquals(incrementErrorMsg, oldVal, newVal);
  }

  private void assertIncrementInRemoteReadOps(long oldVal, long newVal) {
    final String incrementErrorMsg = "Number of remote read ops should increase";
    Assertions.assertThat(newVal)
            .describedAs(incrementErrorMsg)
            .isGreaterThan(oldVal);
  }

  /**
   * Assert that the data read matches the dataset at the given offset.
   * This helps verify that the seek process is moving the read pointer
   * to the correct location in the file.
   * @param readOffset the offset in the file where the read began.
   * @param operation operation name for the assertion.
   * @param data data read in.
   * @param length length of data to check.
   */
  private void assertDatasetEquals(
          final int readOffset,
          final String operation,
          final byte[] data,
          int length) {
    for (int i = 0; i < length; i++) {
      int o = readOffset + i;
      assertEquals(operation + "with read offset " + readOffset
                      + ": data[" + i + "] != actualData[" + o + "]",
              BLOCK[o], data[i]);
    }
  }
}
