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
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.catalog.NodeDescriptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

/**
 * Class that tracks the mapping between
 * evaluators & the data partition assigned
 * to those evaluators. Its part of the
 * implementation of a {@link org.apache.reef.io.data.loading.api.DataLoadingService}
 * that uses the Hadoop {@link org.apache.hadoop.mapred.InputFormat} to
 * partition the data and request resources
 * accordingly
 * <p/>
 * This is an online version which satisfies
 * requests in a greedy way.
 *
 */
@DriverSide
public class GreedyEvaluatorToPartitionStrategy implements EvaluatorToPartitionStrategy<InputSplit> {
  private static final Logger LOG = Logger
      .getLogger(GreedyEvaluatorToPartitionStrategy.class.getName());

  private final ConcurrentMap<String, BlockingQueue<NumberedSplit<InputSplit>>> locationToSplits = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, NumberedSplit<InputSplit>> evaluatorToSplits = new ConcurrentHashMap<>();
  private final BlockingQueue<NumberedSplit<InputSplit>> unallocatedSplits = new LinkedBlockingQueue<>();


  @Inject
  GreedyEvaluatorToPartitionStrategy() {
  }

  /**
   * Initializes the locations of splits mapping.
   *
   * @param splits
   */
  @Override
  public void init(final Map<InputFolder, InputSplit[]> splitsPerFolder) {
    final Pair<InputSplit[],InputFolder[]> splitsAndFolders = getSplitsAndFolders(splitsPerFolder);
    final InputSplit[] splits = splitsAndFolders.getFirst();
    final InputFolder[] folders = splitsAndFolders.getSecond();
    Validate.isTrue(splits.length == folders.length);
    try {
      for (int splitNum = 0; splitNum < splitsAndFolders.getFirst().length; splitNum++) {
        LOG.log(Level.FINE, "Processing split: " + splitNum);
        final InputSplit split = splits[splitNum];
        final String[] locations = split.getLocations();
        final NumberedSplit<InputSplit> numberedSplit = new NumberedSplit<InputSplit>(split, splitNum, folders[splitNum]);
        unallocatedSplits.add(numberedSplit);
        for (final String location : locations) {
          BlockingQueue<NumberedSplit<InputSplit>> newSplitQue = new LinkedBlockingQueue<NumberedSplit<InputSplit>>();
          final BlockingQueue<NumberedSplit<InputSplit>> splitQue = locationToSplits.putIfAbsent(location,
              newSplitQue);
          if (splitQue != null) {
            newSplitQue = splitQue;
          }
          newSplitQue.add(numberedSplit);
        }
      }
      for (final Map.Entry<String, BlockingQueue<NumberedSplit<InputSplit>>> locSplit : locationToSplits.entrySet()) {
        LOG.log(Level.FINE, locSplit.getKey() + ": " + locSplit.getValue().toString());
      }
    } catch (final IOException e) {
      throw new RuntimeException(
          "Unable to get InputSplits using the specified InputFormat", e);
    }
  }

  private Pair<InputSplit[], InputFolder[]> getSplitsAndFolders(
      final Map<InputFolder, InputSplit[]> splitsPerFolder) {
    final List<InputSplit> inputSplits = new ArrayList<>();
    final List<InputFolder> inputFolder = new ArrayList<>();
    for (final Entry<InputFolder, InputSplit[]> entry : splitsPerFolder
        .entrySet()) {
      final InputFolder inFolder = entry.getKey();
      final InputSplit[] splits = entry.getValue();
      for (final InputSplit split : splits) {
        inputSplits.add(split);
        inputFolder.add(inFolder);
      }
    }
    return new Pair<>(inputSplits.toArray(new InputSplit[inputSplits.size()]),
        inputFolder.toArray(new InputFolder[inputFolder.size()]));
  }

  /**
   * Get an input split to be assigned to this.
   * evaluator
   * <p/>
   * Allocates one if its not already allocated
   *
   * @param evaluatorId
   * @return
   */
  @Override
  public NumberedSplit<InputSplit> getInputSplit(final NodeDescriptor nodeDescriptor, final String evaluatorId) {
    final String hostName = nodeDescriptor.getName();
    synchronized (evaluatorToSplits) {
      if (evaluatorToSplits.containsKey(evaluatorId)) {
        LOG.log(Level.FINE, "Found an already allocated partition");
        LOG.log(Level.FINE, evaluatorToSplits.toString());
        return evaluatorToSplits.get(evaluatorId);
      }
    }
    LOG.log(Level.FINE, "allocated partition not found");
    if (locationToSplits.containsKey(hostName)) {
      LOG.log(Level.FINE, "Found partitions possibly hosted for " + evaluatorId + " at " + hostName);
      final NumberedSplit<InputSplit> split = allocateSplit(evaluatorId, locationToSplits.get(hostName));
      LOG.log(Level.FINE, evaluatorToSplits.toString());
      if (split != null) {
        return split;
      }
    }
    //pick random
    LOG.log(
        Level.FINE,
        hostName
            + " does not host any partitions or someone else took partitions hosted here. Picking a random one");
    final NumberedSplit<InputSplit> split = allocateSplit(evaluatorId, unallocatedSplits);
    LOG.log(Level.FINE, evaluatorToSplits.toString());
    if (split != null) {
      return split;
    }
    throw new RuntimeException("Unable to find an input partition to evaluator " + evaluatorId);
  }

  private NumberedSplit<InputSplit> allocateSplit(final String evaluatorId,
                                         final BlockingQueue<NumberedSplit<InputSplit>> value) {
    if (value == null) {
      LOG.log(Level.FINE, "Queue of splits can't be empty. Returning null");
      return null;
    }
    while (true) {
      final NumberedSplit<InputSplit> split = value.poll();
      if (split == null) {
        return null;
      }
      if (value == unallocatedSplits || unallocatedSplits.remove(split)) {
        LOG.log(Level.FINE, "Found split-" + split.getIndex() + " in the queue");
        final NumberedSplit<InputSplit> old = evaluatorToSplits.putIfAbsent(evaluatorId, split);
        if (old != null) {
          final String msg = "Trying to assign different partitions to the same evaluator " +
              "is not supported";
          LOG.severe(msg);
          throw new RuntimeException(msg);
        } else {
          LOG.log(Level.FINE, "Returning " + split.getIndex());
          return split;
        }
      }
    }
  }
}
