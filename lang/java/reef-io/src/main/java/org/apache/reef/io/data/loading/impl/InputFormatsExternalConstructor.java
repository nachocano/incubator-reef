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

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.ExternalConstructor;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;


/**
 * A Tang External Constructor to inject the required.
 * InputFormats
 */
@DriverSide
public class InputFormatsExternalConstructor implements ExternalConstructor<InputFormats> {

  private final JobConfs jobConfs;

  @Inject
  public InputFormatsExternalConstructor(final JobConfs jobConfs) {
    this.jobConfs = jobConfs;
  }

  @Override
  public InputFormats newInstance() {
    final List<InputFormat<?, ?>> inputFormats = new ArrayList<>(jobConfs.size());
    for (final JobConf jobConf: jobConfs.getJobConfs()) {
      final ExternalConstructor<InputFormat<?, ?>> inputFormatConstructor = new InputFormatExternalConstructor(jobConf);
      inputFormats.add(inputFormatConstructor.newInstance());
    }
    return new InputFormats(inputFormats);
  }
}
