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

import org.apache.commons.lang.Validate;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.io.data.loading.impl.EvaluatorRequestSerializer;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.impl.SingleThreadStage;
import org.apache.reef.wake.time.Clock;
import org.apache.reef.wake.time.event.Alarm;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The driver component for the DataLoadingService
 * Also acts as the central point for resource requests
 * All the allocated evaluators pass through this and
 * the ones that need data loading have a context stacked
 * that enables a task to get access to Data via the
 * {@link DataSet}.
 * <p/>
 * TODO: Add timeouts
 */
@DriverSide
@Unit
public class DataLoader {

  private static final Logger LOG = Logger.getLogger(DataLoader.class.getName());

  private final ConcurrentMap<String, Pair<Configuration, Configuration>> submittedDataEvalConfigs = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Configuration> submittedComputeEvalConfigs = new ConcurrentHashMap<>();
  private final BlockingQueue<Configuration> failedComputeEvalConfigs = new LinkedBlockingQueue<>();
  private final BlockingQueue<Pair<Configuration, Configuration>> failedDataEvalConfigs = new LinkedBlockingQueue<>();

  private final AtomicInteger numComputeRequestsToSubmit = new AtomicInteger(0);
  private final AtomicInteger numDataRequestsToSubmit = new AtomicInteger(0);

  private final DataLoadingService dataLoadingService;
  private int dataEvalMemoryMB;
  private int dataEvalCore;
  private final SingleThreadStage<EvaluatorRequest> resourceRequestStage;
  private final ResourceRequestHandler resourceRequestHandler;
  private int computeEvalMemoryMB;
  private int computeEvalCore;
  private final EvaluatorRequestor requestor;

  @Inject
  public DataLoader(
      final Clock clock,
      final EvaluatorRequestor requestor,
      final DataLoadingService dataLoadingService,
      @Parameter(DataLoadingRequestBuilder.DataLoadingEvaluatorMemoryMB.class) final int dataEvalMemoryMB,
      @Parameter(DataLoadingRequestBuilder.DataLoadingEvaluatorNumberOfCores.class) final int dataEvalCore,
      @Parameter(DataLoadingRequestBuilder.DataLoadingComputeRequest.class) final String serializedComputeRequest) {
    this(clock, requestor, dataLoadingService, new HashSet<String>(Arrays.asList(serializedComputeRequest)),
        new HashSet<String>(Arrays.asList(EvaluatorRequestSerializer.serialize(EvaluatorRequest.newBuilder()
            .setMemory(dataEvalMemoryMB).setNumberOfCores(dataEvalCore).build()))));
  }

  @Inject
  private DataLoader(
      final Clock clock,
      final EvaluatorRequestor requestor,
      final DataLoadingService dataLoadingService,
      @Parameter(DataLoadingRequestBuilder.DataLoadingComputeRequests.class) final Set<String> serializedComputeRequests,
      @Parameter(DataLoadingRequestBuilder.DataLoadingDataRequests.class) final Set<String> serializedDataRequests) {

    Validate.notEmpty(serializedDataRequests, "Should contain a data request object");
    // FIXME: Issue #855: We need this alarm to look busy for REEF.
    clock.scheduleAlarm(30000, new EventHandler<Alarm>() {
      @Override
      public void onNext(final Alarm time) {
        LOG.log(Level.FINE, "Received Alarm: {0}", time);
      }
    });

    this.dataEvalMemoryMB = -1;
    this.dataEvalCore = -1;
    this.requestor = requestor;
    this.dataLoadingService = dataLoadingService;
    this.resourceRequestHandler = new ResourceRequestHandler(requestor);
    this.resourceRequestStage = new SingleThreadStage<>(this.resourceRequestHandler, serializedComputeRequests.size()
        + serializedDataRequests.size());

    // check for both conditions, in case a client used the default compute request
    if (serializedComputeRequests.isEmpty() || (serializedComputeRequests.size() == 1 &&
        DataLoadingRequestBuilder.DataLoadingComputeRequest.DEFAULT_COMPUTE_REQUEST.equals(serializedComputeRequests.iterator().next()))) {
      computeEvalMemoryMB = -1;
      computeEvalCore = 1;
    } else {
      for (final String serializedComputeRequest : serializedComputeRequests) {
        final EvaluatorRequest computeRequest = EvaluatorRequestSerializer.deserialize(serializedComputeRequest);
        this.numComputeRequestsToSubmit.addAndGet(computeRequest.getNumber());
        this.computeEvalMemoryMB = Math.max(this.computeEvalMemoryMB, computeRequest.getMegaBytes());
        this.computeEvalCore = Math.max(this.computeEvalCore, computeRequest.getNumberOfCores());
        this.resourceRequestStage.onNext(computeRequest);
      }
    }

    final int dcs = serializedDataRequests.size();
    final int partitionsPerDataCenter = this.dataLoadingService.getNumberOfPartitions() / dcs;
    int missing = this.dataLoadingService.getNumberOfPartitions() % dcs;
    for (final String serializedDataRequest : serializedDataRequests) {
      EvaluatorRequest dataRequest = EvaluatorRequestSerializer.deserialize(serializedDataRequest);
      this.dataEvalMemoryMB = Math.max(this.dataEvalMemoryMB, dataRequest.getMegaBytes());
      this.dataEvalCore = Math.max(this.dataEvalCore, dataRequest.getNumberOfCores());
      // clone the request but update the number of evaluators based on the number of partitions
      int number = partitionsPerDataCenter;
      if (missing > 0) {
        number++;
        missing--;
      }
      dataRequest = EvaluatorRequest.newBuilder(dataRequest).setNumber(number).build();
      this.numDataRequestsToSubmit.addAndGet(number);
      this.resourceRequestStage.onNext(dataRequest);
    }
  }

  public class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      LOG.log(Level.INFO, "StartTime: {0}", startTime);
      resourceRequestHandler.releaseResourceRequestGate();
    }
  }

  public class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {

    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {

      final String evalId = allocatedEvaluator.getId();
      LOG.log(Level.FINEST, "Allocated evaluator: {0}", evalId);

      if (!failedComputeEvalConfigs.isEmpty()) {
        LOG.log(Level.FINE, "Failed Compute requests need to be satisfied for {0}", evalId);
        final Configuration conf = failedComputeEvalConfigs.poll();
        if (conf != null) {
          LOG.log(Level.FINE, "Satisfying failed configuration for {0}", evalId);
          allocatedEvaluator.submitContext(conf);
          submittedComputeEvalConfigs.put(evalId, conf);
          return;
        }
      }

      if (!failedDataEvalConfigs.isEmpty()) {
        LOG.log(Level.FINE, "Failed Data requests need to be satisfied for {0}", evalId);
        final Pair<Configuration, Configuration> confPair = failedDataEvalConfigs.poll();
        if (confPair != null) {
          LOG.log(Level.FINE, "Satisfying failed configuration for {0}", evalId);
          allocatedEvaluator.submitContextAndService(confPair.first, confPair.second);
          submittedDataEvalConfigs.put(evalId, confPair);
          return;
        }
      }

      final int evaluatorsForComputeRequest = numComputeRequestsToSubmit.decrementAndGet();
      LOG.log(Level.FINE, "Evaluators for compute request: {0}", evaluatorsForComputeRequest);

      if (evaluatorsForComputeRequest >= 0) {
        try {
          final Configuration idConfiguration = ContextConfiguration.CONF
              .set(ContextConfiguration.IDENTIFIER,
                  dataLoadingService.getComputeContextIdPrefix() + evaluatorsForComputeRequest)
              .build();
          LOG.log(Level.FINE, "Submitting Compute Context to {0}", evalId);
          allocatedEvaluator.submitContext(idConfiguration);
          submittedComputeEvalConfigs.put(allocatedEvaluator.getId(), idConfiguration);
          if (evaluatorsForComputeRequest >= 0) {
            LOG.log(Level.FINE, evaluatorsForComputeRequest > 0 ? "More Compute requests need to be satisfied"
                : "All Compute requests satisfied." + " Releasing gate");
            resourceRequestHandler.releaseResourceRequestGate();
          }
        } catch (final BindException e) {
          throw new RuntimeException("Unable to bind context id for Compute request", e);
        }

      } else {

        final int evaluatorsForDataRequest = numDataRequestsToSubmit.decrementAndGet();
        LOG.log(Level.FINE, "Evaluators for data request: {0}", evaluatorsForDataRequest);

        final Pair<Configuration, Configuration> confPair = new Pair<>(
            dataLoadingService.getContextConfiguration(allocatedEvaluator),
            dataLoadingService.getServiceConfiguration(allocatedEvaluator));

        LOG.log(Level.FINE, "Submitting data loading context to {0}", evalId);
        allocatedEvaluator.submitContextAndService(confPair.first, confPair.second);
        submittedDataEvalConfigs.put(allocatedEvaluator.getId(), confPair);

        // don't need to release if if 0
        if (evaluatorsForDataRequest > 0) {
          LOG.log(Level.FINE, "More Data requests need to be satisfied. Releasing gate");
          resourceRequestHandler.releaseResourceRequestGate();
        } else if (evaluatorsForDataRequest == 0) {
          LOG.log(Level.FINE, "All Data requests satisfied");
        }
      }
    }
  }

  public class EvaluatorFailedHandler implements EventHandler<FailedEvaluator> {
    @Override
    public void onNext(final FailedEvaluator failedEvaluator) {

      final String evalId = failedEvaluator.getId();

      final Configuration computeConfig = submittedComputeEvalConfigs.remove(evalId);
      if (computeConfig != null) {

        LOG.log(Level.INFO, "Received failed compute evaluator: {0}", evalId);
        failedComputeEvalConfigs.add(computeConfig);

        requestor.submit(EvaluatorRequest.newBuilder()
            .setMemory(computeEvalMemoryMB).setNumber(1).setNumberOfCores(computeEvalCore).build());

      } else {

        final Pair<Configuration, Configuration> confPair = submittedDataEvalConfigs.remove(evalId);
        if (confPair != null) {

          LOG.log(Level.INFO, "Received failed data evaluator: {0}", evalId);
          failedDataEvalConfigs.add(confPair);

          requestor.submit(EvaluatorRequest.newBuilder()
              .setMemory(dataEvalMemoryMB).setNumber(1).setNumberOfCores(dataEvalCore).build());

        } else {

          LOG.log(Level.SEVERE, "Received unknown failed evaluator " + evalId,
              failedEvaluator.getEvaluatorException());

          throw new RuntimeException("Received failed evaluator that I did not submit: " + evalId);
        }
      }
    }
  }
}
