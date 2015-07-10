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

public class JobConfsExternalConstructor implements ExternalConstructor<LocationAwareJobConfs> {

  private final String inputFormatClassName;
  private final List<InputFolder> inputFolders;

  @Inject
  public JobConfsExternalConstructor(
      @Parameter(JobConfExternalConstructor.InputFormatClass.class) final String inputFormatClassName,
      @Parameter(InputFolders.class) final Set<String> serializedInputFolders) {
    this.inputFormatClassName = inputFormatClassName;
    this.inputFolders = new ArrayList<>(serializedInputFolders.size());
    for (final String serializedInputFolder : serializedInputFolders) {
      this.inputFolders.add(InputFolderSerializer.deserialize(serializedInputFolder));
    }
  }

  @Override
  public LocationAwareJobConfs newInstance() {
    final List<LocationAwareJobConf> locationAwareJobConfs = new ArrayList<>(inputFolders.size());
    final Iterator<InputFolder> it = inputFolders.iterator();
    while (it.hasNext()) {
      final InputFolder inputFolder = it.next();
      final ExternalConstructor<JobConf> jobConf = new JobConfExternalConstructor(inputFormatClassName,
          inputFolder.getPath());
      locationAwareJobConfs.add(new LocationAwareJobConf(jobConf.newInstance(), inputFolder));
    }
    return new LocationAwareJobConfs(locationAwareJobConfs);
  }

  @NamedParameter
  public static final class InputFolders implements Name<Set<String>> {
  }
}
