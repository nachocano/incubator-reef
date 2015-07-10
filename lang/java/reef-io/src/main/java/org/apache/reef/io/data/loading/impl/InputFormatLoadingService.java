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
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.context.ServiceConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.io.data.loading.api.DataLoadingRequestBuilder;
import org.apache.reef.io.data.loading.api.DataLoadingService;
import org.apache.reef.io.data.loading.api.DataSet;
import org.apache.reef.io.data.loading.api.EvaluatorToPartitionStrategy;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.BindException;

import javax.inject.Inject;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An implementation of {@link DataLoadingService}
 * that uses the Hadoop {@link InputFormat} to find
 * partitions of data & request resources.
 * <p/>
 * The InputFormat is taken from the job configurations
 * <p/>
 * The so called EvaluatorToPartitionStrategy is injected,
 * in order to support different ways to achieve data locality
 * @see {@link EvaluatorToPartitionStrategy}
 */
@DriverSide
public class InputFormatLoadingService<K, V> implements DataLoadingService {

  private static final Logger LOG = Logger.getLogger(InputFormatLoadingService.class.getName());

  private static final String DATA_LOAD_CONTEXT_PREFIX = "DataLoadContext-";

  private static final String COMPUTE_CONTEXT_PREFIX =
      "ComputeContext-" + new Random(3381).nextInt(1 << 20) + "-";

  private final EvaluatorToPartitionStrategy<InputSplit> evaluatorToPartitionStrategy;
  private int numberOfPartitions;
  private final Map<String, Integer> numberOfPartitionsPerLocation;
  private final boolean inMemory;
  private final String inputFormatClass;


  /**
   * @deprecated since 0.12. Should use the other constructor instead, which
   *             allows to specify the strategy on how to assign partitions to
   *             evaluators
   *
   */
  @Deprecated
  @Inject
  public InputFormatLoadingService(
      final InputFormat<K, V> inputFormat,
      final JobConf jobConf,
      @Parameter(DataLoadingRequestBuilder.NumberOfDesiredSplits.class) final int numberOfDesiredSplits,
      @Parameter(DataLoadingRequestBuilder.LoadDataIntoMemory.class) final boolean inMemory,
      @Parameter(JobConfExternalConstructor.InputFormatClass.class) final String inputFormatClass,
      @Parameter(JobConfExternalConstructor.InputPath.class) final String inputPath) {
    this(new LocationAwareJobConfs(Arrays.asList(new LocationAwareJobConf(jobConf, new InputFolder(inputPath, InputFolder.ANY)))), new GreedyEvaluatorToPartitionStrategy(), numberOfDesiredSplits, inMemory, inputFormatClass);
  }

  @SuppressWarnings("rawtypes")
  @Inject
  public InputFormatLoadingService(
      final LocationAwareJobConfs locAwareJobConfs,
      final EvaluatorToPartitionStrategy<InputSplit> evaluatorToPartitionStrategy,
      @Parameter(DataLoadingRequestBuilder.NumberOfDesiredSplits.class) final int numberOfDesiredSplits,
      @Parameter(DataLoadingRequestBuilder.LoadDataIntoMemory.class) final boolean inMemory,
      @Parameter(JobConfExternalConstructor.InputFormatClass.class) final String inputFormatClass) {

    this.inMemory = inMemory;
    this.inputFormatClass = inputFormatClass;
    this.evaluatorToPartitionStrategy = evaluatorToPartitionStrategy;
    this.numberOfPartitionsPerLocation = new HashMap<>();

    final Iterator<LocationAwareJobConf> it = locAwareJobConfs.iterator();
    final Map<InputFolder, InputSplit[]> splitsPerFolder = new HashMap<>();
    while (it.hasNext()) {
      final LocationAwareJobConf locAwareJobConf = it.next();
      try {
        final JobConf jobConf = locAwareJobConf.getJobConf();
        final InputFolder inFolder = locAwareJobConf.getInputFolder();
        final InputFormat inputFormat = jobConf.getInputFormat();
        final InputSplit[] inputSplits = inputFormat.getSplits(jobConf, numberOfDesiredSplits);
        splitsPerFolder.put(inFolder, inputSplits);
        if (LOG.isLoggable(Level.FINEST)) {
          LOG.log(Level.FINEST, "Splits for path: {0} {1}", new Object[]{inFolder.getPath(), Arrays.toString(inputSplits)});
        }
        this.numberOfPartitions += inputSplits.length;

      } catch (final IOException e) {
        throw new RuntimeException("Unable to get InputSplits using the specified InputFormat", e);
      }
    }
    this.evaluatorToPartitionStrategy.init(splitsPerFolder);
    LOG.log(Level.FINE, "Number of partitions: {0}", this.numberOfPartitions);
  }

  @Override
  public int getNumberOfPartitions() {
    return this.numberOfPartitions;
  }

  @Override
  public int getNumberOfPartitionsPerLocation(final String location) {
    int result = 0;
    if (this.numberOfPartitionsPerLocation.containsKey(location)) {
      result = this.numberOfPartitionsPerLocation.get(location);
    }
    return result;
  }


  @Override
  public Configuration getContextConfiguration(final AllocatedEvaluator allocatedEvaluator) {

    final NumberedSplit<InputSplit> numberedSplit =
        this.evaluatorToPartitionStrategy.getInputSplit(
            allocatedEvaluator.getEvaluatorDescriptor().getNodeDescriptor(),
            allocatedEvaluator.getId());

    return ContextConfiguration.CONF
        .set(ContextConfiguration.IDENTIFIER, DATA_LOAD_CONTEXT_PREFIX + numberedSplit.getIndex())
        .build();
  }

  @Override
  public Configuration getServiceConfiguration(final AllocatedEvaluator allocatedEvaluator) {

    try {

      final NumberedSplit<InputSplit> numberedSplit =
          this.evaluatorToPartitionStrategy.getInputSplit(
              allocatedEvaluator.getEvaluatorDescriptor().getNodeDescriptor(),
              allocatedEvaluator.getId());

      final Configuration serviceConfiguration = ServiceConfiguration.CONF
          .set(ServiceConfiguration.SERVICES,
              this.inMemory ? InMemoryInputFormatDataSet.class : InputFormatDataSet.class)
          .build();

      return Tang.Factory.getTang().newConfigurationBuilder(serviceConfiguration)
          .bindImplementation(
              DataSet.class,
              this.inMemory ? InMemoryInputFormatDataSet.class : InputFormatDataSet.class)
          .bindNamedParameter(JobConfExternalConstructor.InputFormatClass.class, inputFormatClass)
          .bindNamedParameter(JobConfExternalConstructor.InputPath.class, numberedSplit.getPath())
          .bindNamedParameter(
              InputSplitExternalConstructor.SerializedInputSplit.class,
              WritableSerializer.serialize(numberedSplit.getEntry()))
          .bindConstructor(InputSplit.class, InputSplitExternalConstructor.class)
          .bindConstructor(JobConf.class, JobConfExternalConstructor.class)
          .build();

    } catch (final BindException ex) {
      final String evalId = allocatedEvaluator.getId();
      final String msg = "Unable to create configuration for evaluator " + evalId;
      LOG.log(Level.WARNING, msg, ex);
      throw new RuntimeException(msg, ex);
    }
  }

  @Override
  public String getComputeContextIdPrefix() {
    return COMPUTE_CONTEXT_PREFIX;
  }

  @Override
  public boolean isComputeContext(final ActiveContext context) {
    return context.getId().startsWith(COMPUTE_CONTEXT_PREFIX);
  }

  @Override
  public boolean isDataLoadedContext(final ActiveContext context) {
    return context.getId().startsWith(DATA_LOAD_CONTEXT_PREFIX);
  }

}
