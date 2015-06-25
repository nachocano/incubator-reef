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
package org.apache.reef.tests.rack.awareness;

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tests.TestEnvironment;
import org.apache.reef.tests.TestEnvironmentFactory;
import org.apache.reef.tests.library.driver.OnDriverStartedAllocateOne;
import org.apache.reef.util.EnvironmentUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.logging.Logger;

/**
 * Tests whether an Evaluator receives its rack information. For now, the rack name is hardcoded to "/default-rack".
 * In the future, when the user can specify which racks to execute on, this test will make more sense
 */
public final class RackAwareEvaluatorTest {

  private static final Logger LOG = Logger.getLogger(RackAwareEvaluatorTest.class.getName());
  private final TestEnvironment testEnvironment = TestEnvironmentFactory.getNewTestEnvironment();


  @Before
  public void setUp() throws Exception {
    testEnvironment.setUp();
  }

  @After
  public void tearDown() throws Exception {
    this.testEnvironment.tearDown();
  }

  @Test
  public void testRackAwareEvaluatorRunningOnDefaultRack() {
    //Given
    final Configuration driverConfiguration = DriverConfiguration.CONF
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "TEST_RackAwareEvaluator")
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(RackAwareEvaluatorTestDriver.class))
        .set(DriverConfiguration.ON_DRIVER_STARTED, OnDriverStartedAllocateOne.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, RackAwareEvaluatorTestDriver.EvaluatorAllocatedHandler.class)
        .build();

    // When
    final LauncherStatus status = this.testEnvironment.run(driverConfiguration);
    // Then
    Assert.assertTrue("Job state after execution: " + status, status.isSuccess());
    LOG.info("Success!!!");
  }

}
