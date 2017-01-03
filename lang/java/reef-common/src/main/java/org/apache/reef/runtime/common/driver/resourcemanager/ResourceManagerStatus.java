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
package org.apache.reef.runtime.common.driver.resourcemanager;

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.runtime.common.driver.DriverStatusManager;
import org.apache.reef.runtime.common.driver.evaluator.pojos.State;
import org.apache.reef.runtime.common.driver.idle.DriverIdleManager;
import org.apache.reef.runtime.common.driver.idle.DriverIdlenessSource;
import org.apache.reef.runtime.common.driver.idle.IdleMessage;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages the status of the Resource Manager and tracks whether it is idle.
 */
@DriverSide
@Private
public final class ResourceManagerStatus implements EventHandler<RuntimeStatusEvent>, DriverIdlenessSource {

  private static final Logger LOG = Logger.getLogger(ResourceManagerStatus.class.getName());

  private static final String COMPONENT_NAME = "ResourceManager";

  private static final IdleMessage IDLE_MESSAGE =
      new IdleMessage(COMPONENT_NAME, "No outstanding requests or allocations", true);

  private final ResourceManagerErrorHandler resourceManagerErrorHandler;
  private final DriverStatusManager driverStatusManager;
  private final InjectionFuture<DriverIdleManager> driverIdleManager;

  /** Mutable RM state. */
  private State state = State.INIT;

  /** Number of container requests outstanding with the RM, as per latest RuntimeStatusEvent message. */
  private int outstandingContainerRequests = 0;

  /** Number of containers currently allocated, as per latest RuntimeStatusEvent message. */
  private int containerAllocationCount = 0;

  @Inject
  private ResourceManagerStatus(
      final ResourceManagerErrorHandler resourceManagerErrorHandler,
      final DriverStatusManager driverStatusManager,
      final InjectionFuture<DriverIdleManager> driverIdleManager) {

    this.resourceManagerErrorHandler = resourceManagerErrorHandler;
    this.driverStatusManager = driverStatusManager;
    this.driverIdleManager = driverIdleManager;
  }

  @Override
  public void onNext(final RuntimeStatusEvent runtimeStatusEvent) {

    final State newState = runtimeStatusEvent.getState();

    LOG.log(Level.FINEST, "Runtime status: {0}", runtimeStatusEvent);

    synchronized(this) {
      this.outstandingContainerRequests = runtimeStatusEvent.getOutstandingContainerRequests().orElse(0);
      this.containerAllocationCount = runtimeStatusEvent.getContainerAllocationList().size();

      this.setState(newState);
    }

    switch (newState) {
    case FAILED:
      this.onRMFailure(runtimeStatusEvent);
      break;
    case DONE:
      this.onRMDone(runtimeStatusEvent);
      break;
    case RUNNING:
      this.onRMRunning(runtimeStatusEvent);
      break;
    case INIT:
    case SUSPEND:
    case KILLED:
      break;
    default:
      throw new RuntimeException("Unknown state: " + newState);
    }
  }

  /**
   * Change the state of the Resource Manager to be RUNNING.
   */
  public synchronized void setRunning() {
    this.setState(State.RUNNING);
  }

  /**
   * Driver is idle if, regardless of status, it has no evaluators allocated and no pending container requests.
   * @return true if the driver can be considered idle, false otherwise.
   */
  private synchronized boolean isIdle() {
    return this.outstandingContainerRequests == 0 && this.containerAllocationCount == 0;
  }

  /**
   * Driver is idle if, regardless of status, it has no evaluators allocated
   * and no pending container requests. This method is used in the DriverIdleManager.
   * If all DriverIdlenessSource components are idle, DriverIdleManager will initiate Driver shutdown.
   * @return idle, if there are no outstanding requests or allocations. Not idle otherwise.
   */
  @Override
  public synchronized IdleMessage getIdleStatus() {

    if (this.isIdle()) {
      return IDLE_MESSAGE;
    }

    final String message = String.format(
        "There are %d outstanding container requests and %d allocated containers",
        this.outstandingContainerRequests, this.containerAllocationCount);

    return new IdleMessage(COMPONENT_NAME, message, false);
  }

  private synchronized void onRMFailure(final RuntimeStatusEvent runtimeStatusEvent) {
    assert runtimeStatusEvent.getState() == State.FAILED;
    this.resourceManagerErrorHandler.onNext(runtimeStatusEvent.getError().get());
  }

  private synchronized void onRMDone(final RuntimeStatusEvent runtimeStatusEvent) {
    assert runtimeStatusEvent.getState() == State.DONE;
    LOG.log(Level.INFO, "Resource Manager shutdown happened. Triggering Driver shutdown.");
    this.driverStatusManager.onComplete();
  }

  private void onRMRunning(final RuntimeStatusEvent runtimeStatusEvent) {
    assert runtimeStatusEvent.getState() == State.RUNNING;
    if (this.isIdle()) {
      this.driverIdleManager.get().onPotentiallyIdle(IDLE_MESSAGE);
    }
  }

  private synchronized void setState(final State toState) {
    if (this.state == toState) {
      LOG.log(Level.FINE, "Transition from {0} state to the same state.", this.state);
    } else if (this.state.isLegalTransition(toState)) {
      LOG.log(Level.FINEST, "State transition: {0} -> {1}", new State[] {this.state, toState});
      this.state = toState;
    } else {
      throw new IllegalStateException(
          "Resource manager attempts illegal state transition from " + this.state + " to " + toState);
    }
  }
}
