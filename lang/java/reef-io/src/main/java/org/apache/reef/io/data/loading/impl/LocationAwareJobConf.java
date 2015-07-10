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

public final class LocationAwareJobConf {

  private final JobConf jobConf;
  private final InputFolder inputFolder;

  public LocationAwareJobConf(final JobConf jobConf, final InputFolder inputFolder) {
    Validate.notNull(jobConf);
    Validate.notNull(inputFolder);
    this.jobConf = jobConf;
    this.inputFolder = inputFolder;
  }

  public JobConf getJobConf() {
    return jobConf;
  }

  public InputFolder getInputFolder() {
    return inputFolder;
  }

}
