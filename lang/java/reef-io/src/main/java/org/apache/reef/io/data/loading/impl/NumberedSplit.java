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
import org.apache.reef.io.data.loading.api.EvaluatorToPartitionStrategy;

/**
 * A tuple of an object of type E and an integer index.
 * Used inside {@link EvaluatorToPartitionStrategy} implementations to
 * mark the partitions associated with each {@link org.apache.hadoop.mapred.InputSplit}
 *
 * @param <E>
 */
public final class NumberedSplit<E> implements Comparable<NumberedSplit<E>> {
  private final E entry;
  private final int index;
  private final InputFolder folder;

  public NumberedSplit(final E entry, final int index, final InputFolder folder) {
    Validate.notNull(entry, "Entry cannot be null");
    Validate.notNull(folder, "Folder cannot be null");
    this.entry = entry;
    this.index = index;
    this.folder = folder;
  }

  public String getPath() {
    return folder.getPath();
  }

  public E getEntry() {
    return entry;
  }

  public int getIndex() {
    return index;
  }

  @Override
  public String toString() {
    return "InputSplit-" + index;
  }

  @Override
  public int compareTo(final NumberedSplit<E> o) {
    if (this.index == o.index) {
      return 0;
    }
    if (this.index < o.index) {
      return -1;
    }
    return 1;
  }
}
