/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.io.data.loading.api;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.reef.annotations.Unstable;

/**
 * Represents a text input format which gives you the exact number of splits you
 * ask for. It changes the minSplitSize in order to achieve that, based on the
 * file sizes and the desired number of splits.
 *
 */
@Unstable
public final class ExactSplitTextInputFormat extends TextInputFormat {

  private static final Logger LOG = Logger.getLogger(ExactSplitTextInputFormat.class.getName());

  @Override
  public InputSplit[] getSplits(final JobConf job, final int numSplits) throws IOException {
    long totalSize = getTotalSize(job);
    LOG.log(Level.INFO, "Total Size {0}, NumSplits {1}", new Object[]{totalSize, numSplits});
    String splitMinSize = Long.toString((long) (Math.ceil((double) totalSize / numSplits)));
    LOG.log(Level.INFO, "Setting split minimum size to {0}", splitMinSize);
    job.set(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MINSIZE, splitMinSize);
    return super.getSplits(job, numSplits);
  }

  private long getTotalSize(final JobConf job) throws IOException { 
    FileStatus[] files = listStatus(job);
    long totalSize = 0; // compute total size
    for (FileStatus file : files) { // check we have valid files
      if (file.isDirectory()) {
        throw new IOException("Not a file: " + file.getPath());
      }
      totalSize += file.getLen();
    }
    return totalSize;
  }

}
