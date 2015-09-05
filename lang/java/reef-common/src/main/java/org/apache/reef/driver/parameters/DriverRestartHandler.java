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
package org.apache.reef.driver.parameters;

import org.apache.reef.driver.restart.DriverRestarted;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.wake.EventHandler;

import java.util.Set;

/**
 * The EventHandler invoked on Driver restart. Provides the set of Evaluator IDs of Evaluators that are expected to
 * report back to the Driver on restart as well as the time of restart.
 */
@NamedParameter(doc = "The EventHandler invoked on Driver restart. Provides the set of Evaluator IDs of " +
    "Evaluators that are expected to report back to the Driver on restart as well as the time of restart.")
public final class DriverRestartHandler implements Name<Set<EventHandler<DriverRestarted>>> {
}
