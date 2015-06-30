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
package org.apache.reef.driver.catalog;

import org.apache.reef.annotations.Unstable;
import org.apache.reef.driver.evaluator.EvaluatorRequest;

import java.util.Collection;

/**
 * A catalog of the resources available to a REEF instance.
 * <p/>
 * This catalog contains static information about the resources and does not
 * reflect that dynamic availability of resources. In other words: Its entries
 * are an upper bound to what is available to a REEF {@link Driver} at any given
 * moment in time.
 */
@Unstable
public interface ResourceCatalog {

  /**
   * The global list of resources.
   *
   * @return a list of all the static resources available. This is an upper
   * bound.
   */
  Collection<NodeDescriptor> getNodes();

  /**
   * The global list of racks.
   *
   * @return list of all rack descriptors
   */
  Collection<RackDescriptor> getRacks();

  /**
   * Get the node descriptor with the given identifier.
   *
   * @param id of the node.
   * @return the node descriptor assigned to the identifier.
   */
  NodeDescriptor getNode(String nodeId);

  /**
   * We are not using this. It will be removed in the future. In order to do
   * evaluator requests with specific rack names or node names, you should take
   * a look at {@link EvaluatorRequest} new API.
   *
   */
  @Deprecated
  public interface Descriptor {

    String getName();

  }

}
