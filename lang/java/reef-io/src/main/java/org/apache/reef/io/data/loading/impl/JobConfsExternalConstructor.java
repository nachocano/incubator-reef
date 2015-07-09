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

import org.apache.hadoop.mapred.JobConf;
import org.apache.reef.tang.ExternalConstructor;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

public class JobConfsExternalConstructor implements ExternalConstructor<JobConfs> {

  private static final Logger LOG = Logger.getLogger(JobConfsExternalConstructor.class.getName());

  private final String inputFormatClassName;
  private final Set<String> inputPaths;

  @Inject
  public JobConfsExternalConstructor(
      @Parameter(JobConfExternalConstructor.InputFormatClass.class) final String inputFormatClassName,
      @Parameter(InputPaths.class) final Set<String> inputPaths) {
    this.inputFormatClassName = inputFormatClassName;
    this.inputPaths = inputPaths;
  }

  @Override
  public JobConfs newInstance() {
    final List<JobConf> jobConfs = new ArrayList<JobConf>(inputPaths.size());

    final Iterator<String> it = inputPaths.iterator();
    while (it.hasNext()) {
      final ExternalConstructor<JobConf> jobConf = new JobConfExternalConstructor(inputFormatClassName, it.next());
      jobConfs.add(jobConf.newInstance());
    }
    return new JobConfs(jobConfs);
  }

  @NamedParameter(default_values = { "NULL" })
  public static final class InputPaths implements Name<Set<String>> {
  }
}
