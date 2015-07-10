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
import org.apache.reef.io.data.loading.api.EvaluatorToPartitionStrategy;

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
 * This is an abstract class useful for {@link EvaluatorToPartitionStrategy}
 * implementations. Contains some template methods that should be implemented by
 * subclasses. If you implementation does not need this logic, you should just
 * implement the {@link EvaluatorToPartitionStrategy} interface and do not
 * extend this class
 */
@DriverSide
public abstract class AbstractEvaluatorToPartitionStrategy implements EvaluatorToPartitionStrategy<InputSplit> {
  private static final Logger LOG = Logger
      .getLogger(AbstractEvaluatorToPartitionStrategy.class.getName());

  protected final ConcurrentMap<String, BlockingQueue<NumberedSplit<InputSplit>>> locationToSplits = new ConcurrentHashMap<>();
  protected final ConcurrentMap<String, NumberedSplit<InputSplit>> evaluatorToSplits = new ConcurrentHashMap<>();
  protected final BlockingQueue<NumberedSplit<InputSplit>> unallocatedSplits = new LinkedBlockingQueue<>();


  @Inject
  AbstractEvaluatorToPartitionStrategy() {
    LOG.fine("AbstractEvaluatorToPartitionStrategy injected");
  }

  /**
   * Initializes the locations of splits mapping.
   *
   * @param splitsPerFolder
   *          a map containing the input splits per input folder
   */
  @Override
  public void init(final Map<InputFolder, InputSplit[]> splitsPerFolder) {
    final Pair<InputSplit[], InputFolder[]> splitsAndFolders = getSplitsAndFolders(splitsPerFolder);
    final InputSplit[] splits = splitsAndFolders.getFirst();
    final InputFolder[] folders = splitsAndFolders.getSecond();
    Validate.isTrue(splits.length == folders.length);
    for (int splitNum = 0; splitNum < splits.length; splitNum++) {
      LOG.log(Level.FINE, "Processing split: " + splitNum);
      final InputSplit split = splits[splitNum];
      final NumberedSplit<InputSplit> numberedSplit = new NumberedSplit<InputSplit>(split, splitNum, folders[splitNum]);
      unallocatedSplits.add(numberedSplit);
      updateLocations(split, numberedSplit);
    }
    for (final Map.Entry<String, BlockingQueue<NumberedSplit<InputSplit>>> locSplit : locationToSplits.entrySet()) {
      LOG.log(Level.FINE, locSplit.getKey() + ": " + locSplit.getValue().toString());
    }
  }

  protected abstract void updateLocations(final InputSplit split, final NumberedSplit<InputSplit> numberedSplit);
  protected abstract NumberedSplit<InputSplit> tryAllocate(NodeDescriptor nodeDescriptor, String evaluatorId);

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
    synchronized (evaluatorToSplits) {
      if (evaluatorToSplits.containsKey(evaluatorId)) {
        LOG.log(Level.FINE, "Found an already allocated partition");
        LOG.log(Level.FINE, evaluatorToSplits.toString());
        return evaluatorToSplits.get(evaluatorId);
      }
    }
    // first try to allocate based on the hostName
    final String hostName = nodeDescriptor.getName();
    LOG.log(Level.FINE, "Allocated partition not found, trying on {0}", hostName);
    if (locationToSplits.containsKey(hostName)) {
      LOG.log(Level.FINE, "Found partitions possibly hosted for {0} at {1}", new Object[]{ evaluatorId, hostName});
      final NumberedSplit<InputSplit> split = allocateSplit(evaluatorId, locationToSplits.get(hostName));
      if (split != null) {
        return split;
      }
    }
    LOG.log(Level.FINE,
        "{0} does not host any partitions or someone else took partitions hosted here. Picking other ones", hostName);
    final NumberedSplit<InputSplit> split = tryAllocate(nodeDescriptor, evaluatorId);
    if (split == null) {
      throw new RuntimeException("Unable to find an input partition to evaluator " + evaluatorId);
    } else {
      LOG.log(Level.FINE, evaluatorToSplits.toString());
    }
    return split;
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

  protected NumberedSplit<InputSplit> allocateSplit(final String evaluatorId,
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
          throw new RuntimeException("Trying to assign different partitions to the same evaluator is not supported");
        } else {
          LOG.log(Level.FINE, "Returning " + split.getIndex());
          return split;
        }
      }
    }
  }
}
