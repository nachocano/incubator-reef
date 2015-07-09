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
package org.apache.reef.io.data.loading.impl;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.mapred.JobConf;

import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

public class JobConfs {

  private static final Logger LOG = Logger.getLogger(JobConfs.class.getName());

  private final List<JobConf> jobConfs;

  public JobConfs(final List<JobConf> jobConfs) {
    Validate.notEmpty(jobConfs);
    this.jobConfs = jobConfs;
  }

  List<JobConf> getJobConfs() {
    return Collections.unmodifiableList(jobConfs);
  }

  int size() {
    return jobConfs.size();
  }

}
