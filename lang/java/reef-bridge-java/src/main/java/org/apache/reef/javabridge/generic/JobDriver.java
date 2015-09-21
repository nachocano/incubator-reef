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
package org.apache.reef.javabridge.generic;

import org.apache.reef.driver.client.JobMessageObserver;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ClosedContext;
import org.apache.reef.driver.context.ContextMessage;
import org.apache.reef.driver.context.FailedContext;
import org.apache.reef.driver.evaluator.*;
import org.apache.reef.driver.restart.DriverRestarted;
import org.apache.reef.driver.task.*;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.javabridge.*;
import org.apache.reef.driver.restart.DriverRestartCompleted;
import org.apache.reef.runtime.common.driver.DriverStatusManager;
import org.apache.reef.driver.evaluator.EvaluatorProcess;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.util.Optional;
import org.apache.reef.util.logging.CLRBufferedLogHandler;
import org.apache.reef.util.logging.LoggingScope;
import org.apache.reef.util.logging.LoggingScopeFactory;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.remote.impl.ObjectSerializableCodec;
import org.apache.reef.wake.time.Clock;
import org.apache.reef.wake.time.event.Alarm;
import org.apache.reef.wake.time.event.StartTime;
import org.apache.reef.wake.time.event.StopTime;
import org.apache.reef.webserver.*;

import javax.inject.Inject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Generic job driver for CLRBridge.
 */
@Unit
public final class JobDriver {

  private static final Logger LOG = Logger.getLogger(JobDriver.class.getName());
  /**
   * String codec is used to encode the results
   * before passing them back to the client.
   */
  private static final ObjectSerializableCodec<String> JVM_CODEC = new ObjectSerializableCodec<>();
  private final InteropLogger interopLogger = new InteropLogger();
  private final NameServer nameServer;
  private final String nameServerInfo;
  private final HttpServer httpServer;
  private final ActiveContextBridgeFactory activeContextBridgeFactory;
  private final AllocatedEvaluatorBridgeFactory allocatedEvaluatorBridgeFactory;

  /**
   * Wake clock is used to schedule periodical job check-ups.
   */
  private final Clock clock;
  /**
   * Job observer on the client.
   * We use it to send results from the driver back to the client.
   */
  private final JobMessageObserver jobMessageObserver;
  /**
   * Job driver uses EvaluatorRequestor
   * to request Evaluators that will run the Tasks.
   */
  private final EvaluatorRequestor evaluatorRequestor;

  /**
   * Driver status manager to monitor driver status.
   */
  private final DriverStatusManager driverStatusManager;

  /**
   * Factory to setup new CLR process configurations.
   */
  private final CLRProcessFactory clrProcessFactory;

  /**
   * Shell execution results from each Evaluator.
   */
  private final List<String> results = new ArrayList<>();
  /**
   * Map from context ID to running evaluator context.
   */
  private final Map<String, ActiveContext> contexts = new HashMap<>();

  private final REEFFileNames reefFileNames;
  private final LocalAddressProvider localAddressProvider;
  /**
   * Logging scope factory that provides LoggingScope.
   */
  private final LoggingScopeFactory loggingScopeFactory;

  private long allocatedEvaluatorHandler = 0;
  private long activeContextHandler = 0;
  private long taskMessageHandler = 0;
  private long failedTaskHandler = 0;
  private long failedEvaluatorHandler = 0;
  private long httpServerEventHandler = 0;
  private long completedTaskHandler = 0;
  private long runningTaskHandler = 0;
  private long suspendedTaskHandler = 0;
  private long completedEvaluatorHandler = 0;
  private long closedContextHandler = 0;
  private long failedContextHandler = 0;
  private long contextMessageHandler = 0;
  private long driverRestartActiveContextHandler = 0;
  private long driverRestartRunningTaskHandler = 0;
  private long driverRestartCompletedHandler = 0;
  private long driverRestartFailedEvaluatorHandler = 0;
  private boolean clrBridgeSetup = false;
  private boolean isRestarted = false;
  // We are holding on to following on bridge side.
  // Need to add references here so that GC does not collect them.
  private final HashMap<String, AllocatedEvaluatorBridge> allocatedEvaluatorBridges =
      new HashMap<String, AllocatedEvaluatorBridge>();
  private EvaluatorRequestorBridge evaluatorRequestorBridge;


  /**
   * Job driver constructor.
   * All parameters are injected from TANG automatically.
   *
   * @param clock                      Wake clock to schedule and check up running jobs.
   * @param jobMessageObserver         is used to send messages back to the client.
   * @param evaluatorRequestor         is used to request Evaluators.
   * @param activeContextBridgeFactory
   */
  @Inject
  JobDriver(final Clock clock,
            final HttpServer httpServer,
            final NameServer nameServer,
            final JobMessageObserver jobMessageObserver,
            final EvaluatorRequestor evaluatorRequestor,
            final DriverStatusManager driverStatusManager,
            final LoggingScopeFactory loggingScopeFactory,
            final LocalAddressProvider localAddressProvider,
            final ActiveContextBridgeFactory activeContextBridgeFactory,
            final REEFFileNames reefFileNames,
            final AllocatedEvaluatorBridgeFactory allocatedEvaluatorBridgeFactory,
            final CLRProcessFactory clrProcessFactory) {
    this.clock = clock;
    this.httpServer = httpServer;
    this.jobMessageObserver = jobMessageObserver;
    this.evaluatorRequestor = evaluatorRequestor;
    this.nameServer = nameServer;
    this.driverStatusManager = driverStatusManager;
    this.activeContextBridgeFactory = activeContextBridgeFactory;
    this.allocatedEvaluatorBridgeFactory = allocatedEvaluatorBridgeFactory;
    this.nameServerInfo = localAddressProvider.getLocalAddress() + ":" + this.nameServer.getPort();
    this.loggingScopeFactory = loggingScopeFactory;
    this.reefFileNames = reefFileNames;
    this.localAddressProvider = localAddressProvider;
    this.clrProcessFactory = clrProcessFactory;
  }

  private void setupBridge(final ClrHandlersInitializer initializer) {
    // Signal to the clr buffered log handler that the driver has started and that
    // we can begin logging
    LOG.log(Level.INFO, "Initializing CLRBufferedLogHandler...");
    try (final LoggingScope lb = this.loggingScopeFactory.setupBridge()) {
      final CLRBufferedLogHandler handler = getCLRBufferedLogHandler();
      if (handler == null) {
        LOG.log(Level.WARNING, "CLRBufferedLogHandler could not be initialized");
      } else {
        handler.setDriverInitialized();
        LOG.log(Level.INFO, "CLRBufferedLogHandler init complete.");
      }

      final String portNumber = httpServer == null ? null : Integer.toString((httpServer.getPort()));
      if (portNumber != null){
        try {
          final File outputFileName = new File(reefFileNames.getDriverHttpEndpoint());
          BufferedWriter out = new BufferedWriter(new FileWriter(outputFileName));
          out.write(localAddressProvider.getLocalAddress() + ":" + portNumber + "\n");
          out.close();
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
      }

      this.evaluatorRequestorBridge =
          new EvaluatorRequestorBridge(JobDriver.this.evaluatorRequestor, false, loggingScopeFactory);
      final long[] handlers = initializer.getClrHandlers(portNumber, evaluatorRequestorBridge);
      if (handlers != null) {
        if (handlers.length != NativeInterop.N_HANDLERS) {
          throw new RuntimeException(
              String.format("%s handlers initialized in CLR while native bridge is expecting %s handlers",
                  String.valueOf(handlers.length),
                  String.valueOf(NativeInterop.N_HANDLERS)));
        }
        this.allocatedEvaluatorHandler = handlers[NativeInterop.HANDLERS.get(NativeInterop.ALLOCATED_EVALUATOR_KEY)];
        this.activeContextHandler = handlers[NativeInterop.HANDLERS.get(NativeInterop.ACTIVE_CONTEXT_KEY)];
        this.taskMessageHandler = handlers[NativeInterop.HANDLERS.get(NativeInterop.TASK_MESSAGE_KEY)];
        this.failedTaskHandler = handlers[NativeInterop.HANDLERS.get(NativeInterop.FAILED_TASK_KEY)];
        this.failedEvaluatorHandler = handlers[NativeInterop.HANDLERS.get(NativeInterop.FAILED_EVALUATOR_KEY)];
        this.httpServerEventHandler = handlers[NativeInterop.HANDLERS.get(NativeInterop.HTTP_SERVER_KEY)];
        this.completedTaskHandler = handlers[NativeInterop.HANDLERS.get(NativeInterop.COMPLETED_TASK_KEY)];
        this.runningTaskHandler = handlers[NativeInterop.HANDLERS.get(NativeInterop.RUNNING_TASK_KEY)];
        this.suspendedTaskHandler = handlers[NativeInterop.HANDLERS.get(NativeInterop.SUSPENDED_TASK_KEY)];
        this.completedEvaluatorHandler = handlers[NativeInterop.HANDLERS.get(NativeInterop.COMPLETED_EVALUATOR_KEY)];
        this.closedContextHandler = handlers[NativeInterop.HANDLERS.get(NativeInterop.CLOSED_CONTEXT_KEY)];
        this.failedContextHandler = handlers[NativeInterop.HANDLERS.get(NativeInterop.FAILED_CONTEXT_KEY)];
        this.contextMessageHandler = handlers[NativeInterop.HANDLERS.get(NativeInterop.CONTEXT_MESSAGE_KEY)];
        this.driverRestartActiveContextHandler =
            handlers[NativeInterop.HANDLERS.get(NativeInterop.DRIVER_RESTART_ACTIVE_CONTEXT_KEY)];
        this.driverRestartRunningTaskHandler =
            handlers[NativeInterop.HANDLERS.get(NativeInterop.DRIVER_RESTART_RUNNING_TASK_KEY)];
        this.driverRestartCompletedHandler =
            handlers[NativeInterop.HANDLERS.get(NativeInterop.DRIVER_RESTART_COMPLETED_KEY)];
        this.driverRestartFailedEvaluatorHandler =
            handlers[NativeInterop.HANDLERS.get(NativeInterop.DRIVER_RESTART_FAILED_EVALUATOR_KEY)];
      }

      try (final LoggingScope lp =
               this.loggingScopeFactory.getNewLoggingScope("setupBridge::clrSystemHttpServerHandlerOnNext")) {
        final HttpServerEventBridge httpServerEventBridge = new HttpServerEventBridge("SPEC");
        NativeInterop.clrSystemHttpServerHandlerOnNext(this.httpServerEventHandler, httpServerEventBridge,
            this.interopLogger);
        final String specList = httpServerEventBridge.getUriSpecification();
        LOG.log(Level.INFO, "Starting http server, getUriSpecification: {0}", specList);
        if (specList != null) {
          final String[] specs = specList.split(":");
          for (final String s : specs) {
            final HttpHandler h = new HttpServerBridgeEventHandler();
            h.setUriSpecification(s);
            this.httpServer.addHttpHandler(h);
          }
        }
      }
      this.clrBridgeSetup = true;
    }
    LOG.log(Level.INFO, "CLR Bridge setup.");
  }

  private CLRBufferedLogHandler getCLRBufferedLogHandler() {
    for (final Handler handler : Logger.getLogger("").getHandlers()) {
      if (handler instanceof CLRBufferedLogHandler) {
        return (CLRBufferedLogHandler) handler;
      }
    }
    return null;
  }

  private void submitEvaluator(final AllocatedEvaluator eval, final EvaluatorProcess process) {
    synchronized (JobDriver.this) {
      eval.setProcess(process);
      LOG.log(Level.INFO, "Allocated Evaluator: {0}, total running running {1}",
          new Object[]{eval.getId(), JobDriver.this.contexts.size()});
      if (JobDriver.this.allocatedEvaluatorHandler == 0) {
        throw new RuntimeException("Allocated Evaluator Handler not initialized by CLR.");
      }
      final AllocatedEvaluatorBridge allocatedEvaluatorBridge =
          this.allocatedEvaluatorBridgeFactory.getAllocatedEvaluatorBridge(eval, this.nameServerInfo);
      allocatedEvaluatorBridges.put(allocatedEvaluatorBridge.getId(), allocatedEvaluatorBridge);
      NativeInterop.clrSystemAllocatedEvaluatorHandlerOnNext(JobDriver.this.allocatedEvaluatorHandler,
          allocatedEvaluatorBridge, this.interopLogger);
    }
  }

  private void handleFailedEvaluator(final FailedEvaluator eval, final boolean isRestartFailed) {
    try (final LoggingScope ls = loggingScopeFactory.evaluatorFailed(eval.getId())) {
      synchronized (JobDriver.this) {
        LOG.log(Level.SEVERE, "FailedEvaluator", eval);
        for (final FailedContext failedContext : eval.getFailedContextList()) {
          final String failedContextId = failedContext.getId();
          LOG.log(Level.INFO, "removing context " + failedContextId + " from job driver contexts.");
          JobDriver.this.contexts.remove(failedContextId);
        }
        String message = "Evaluator " + eval.getId() + " failed with message: "
            + eval.getEvaluatorException().getMessage();
        JobDriver.this.jobMessageObserver.sendMessageToClient(message.getBytes());

        if (isRestartFailed) {
          evaluatorFailedHandlerWaitForCLRBridgeSetup(driverRestartFailedEvaluatorHandler, eval, isRestartFailed);
        } else {
          evaluatorFailedHandlerWaitForCLRBridgeSetup(failedEvaluatorHandler, eval, isRestartFailed);
        }
      }
    }
  }

  private void evaluatorFailedHandlerWaitForCLRBridgeSetup(final long handle,
                                                           final FailedEvaluator eval,
                                                           final boolean isRestartFailed) {
    if (handle == 0) {
      if (JobDriver.this.clrBridgeSetup) {
        final String message = "No CLR FailedEvaluator handler was set, exiting now";
        LOG.log(Level.WARNING, message);
        JobDriver.this.jobMessageObserver.sendMessageToClient(message.getBytes());
        return;
      } else {
        clock.scheduleAlarm(0, new EventHandler<Alarm>() {
          @Override
          public void onNext(final Alarm time) {
            if (JobDriver.this.clrBridgeSetup) {
              handleFailedEvaluatorInCLR(eval, isRestartFailed);
            } else {
              LOG.log(Level.INFO, "Waiting for CLR bridge to be set up");
              clock.scheduleAlarm(5000, this);
            }
          }
        });
      }
    } else{
      handleFailedEvaluatorInCLR(eval, isRestartFailed);
    }
  }

  private void handleFailedEvaluatorInCLR(final FailedEvaluator eval, final boolean isRestartFailed) {
    final String message = "CLR FailedEvaluator handler set, handling things with CLR handler.";
    LOG.log(Level.INFO, message);
    final FailedEvaluatorBridge failedEvaluatorBridge =
        new FailedEvaluatorBridge(eval, JobDriver.this.evaluatorRequestor,
        JobDriver.this.isRestarted, loggingScopeFactory);
    if (isRestartFailed) {
      NativeInterop.clrSystemDriverRestartFailedEvaluatorHandlerOnNext(
          JobDriver.this.driverRestartFailedEvaluatorHandler, failedEvaluatorBridge, JobDriver.this.interopLogger);
    } else {
      NativeInterop.clrSystemFailedEvaluatorHandlerOnNext(JobDriver.this.failedEvaluatorHandler, failedEvaluatorBridge,
          JobDriver.this.interopLogger);
    }

    final int additionalRequestedEvaluatorNumber = failedEvaluatorBridge.getNewlyRequestedEvaluatorNumber();
    if (additionalRequestedEvaluatorNumber > 0) {
      LOG.log(Level.INFO, "number of additional evaluators requested after evaluator failure: " +
          additionalRequestedEvaluatorNumber);
    }

    JobDriver.this.jobMessageObserver.sendMessageToClient(message.getBytes());
  }

  /**
   * Submit a Task to a single Evaluator.
   */
  private void submit(final ActiveContext context) {
    try {
      LOG.log(Level.INFO, "Send task to context: {0}", new Object[]{context});
      if (JobDriver.this.activeContextHandler == 0) {
        throw new RuntimeException("Active Context Handler not initialized by CLR.");
      }
      final ActiveContextBridge activeContextBridge = activeContextBridgeFactory.getActiveContextBridge(context);
      NativeInterop.clrSystemActiveContextHandlerOnNext(JobDriver.this.activeContextHandler, activeContextBridge,
          JobDriver.this.interopLogger);
    } catch (final Exception ex) {
      LOG.log(Level.SEVERE, "Fail to submit task to active context");
      context.close();
      throw new RuntimeException(ex);
    }
  }

  /**
   * Handles AllocatedEvaluator: Submit an empty context.
   */
  public final class AllocatedEvaluatorHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      try (final LoggingScope ls = loggingScopeFactory.evaluatorAllocated(allocatedEvaluator.getId())) {
        synchronized (JobDriver.this) {
          LOG.log(Level.INFO, "AllocatedEvaluatorHandler.OnNext");
          JobDriver.this.submitEvaluator(allocatedEvaluator, clrProcessFactory.newEvaluatorProcess());
        }
      }
    }
  }

  /**
   * Receive notification that a new Context is available.
   */
  public final class ActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(final ActiveContext context) {
      try (final LoggingScope ls = loggingScopeFactory.activeContextReceived(context.getId())) {
        synchronized (JobDriver.this) {
          LOG.log(Level.INFO, "ActiveContextHandler: Context available: {0}",
              new Object[]{context.getId()});
          JobDriver.this.contexts.put(context.getId(), context);
          JobDriver.this.submit(context);
        }
      }
    }
  }

  /**
   * Receive notification that the Task has completed successfully.
   */
  public final class CompletedTaskHandler implements EventHandler<CompletedTask> {
    @Override
    public void onNext(final CompletedTask task) {
      LOG.log(Level.INFO, "Completed task: {0}", task.getId());
      try (final LoggingScope ls = loggingScopeFactory.taskCompleted(task.getId())) {
        // Take the message returned by the task and add it to the running result.
        String result = "default result";
        try {
          result = new String(task.get());
        } catch (final Exception e) {
          LOG.log(Level.WARNING, "failed to decode task outcome");
        }
        LOG.log(Level.INFO, "Return results to the client:\n{0}", result);
        JobDriver.this.jobMessageObserver.sendMessageToClient(JVM_CODEC.encode(result));
        if (JobDriver.this.completedTaskHandler == 0) {
          LOG.log(Level.INFO, "No CLR handler bound to handle completed task.");
        } else {
          LOG.log(Level.INFO, "CLR CompletedTaskHandler handler set, handling things with CLR handler.");
          final CompletedTaskBridge completedTaskBridge = new CompletedTaskBridge(task, activeContextBridgeFactory);
          NativeInterop.clrSystemCompletedTaskHandlerOnNext(JobDriver.this.completedTaskHandler, completedTaskBridge,
              JobDriver.this.interopLogger);
        }
      }
    }
  }

  /**
   * Receive notification that the entire Evaluator had failed.
   */
  public final class FailedEvaluatorHandler implements EventHandler<FailedEvaluator> {
    @Override
    public void onNext(final FailedEvaluator eval) {
      JobDriver.this.handleFailedEvaluator(eval, false);
      allocatedEvaluatorBridges.remove(eval.getId());
    }
  }

  /**
   * Receive notification that the entire Evaluator had failed on Driver Restart.
   */
  public final class DriverRestartFailedEvaluatorHandler implements EventHandler<FailedEvaluator> {
    @Override
    public void onNext(final FailedEvaluator eval) {
      JobDriver.this.handleFailedEvaluator(eval, true);
    }
  }

  final class HttpServerBridgeEventHandler implements HttpHandler {
    private String uriSpecification;

    /**
     * returns URI specification for the handler.
     */
    @Override
    public String getUriSpecification() {
      return uriSpecification;
    }

    public void setUriSpecification(final String s) {
      uriSpecification = s;
    }

    /**
     * process http request.
     */
    @Override
    public void onHttpRequest(final ParsedHttpRequest parsedHttpRequest, final HttpServletResponse response)
        throws IOException, ServletException {
      LOG.log(Level.INFO, "HttpServerBridgeEventHandler onHttpRequest: {0}", parsedHttpRequest.getRequestUri());
      try (final LoggingScope ls = loggingScopeFactory.httpRequest(parsedHttpRequest.getRequestUri())) {
        final AvroHttpSerializer httpSerializer = new AvroHttpSerializer();
        final AvroHttpRequest avroHttpRequest = httpSerializer.toAvro(parsedHttpRequest);

        final String requestString = httpSerializer.toString(avroHttpRequest);
        final byte[] requestBytes = requestString.getBytes();
        //final byte[] requestBytes = httpSerializer.toBytes(avroHttpRequest);

        try {
          final HttpServerEventBridge httpServerEventBridge = new HttpServerEventBridge(requestBytes);
          NativeInterop.clrSystemHttpServerHandlerOnNext(JobDriver.this.httpServerEventHandler, httpServerEventBridge,
              JobDriver.this.interopLogger);
          final String responseBody = new String(httpServerEventBridge.getQueryResponseData(), "UTF-8");
          response.getWriter().println(responseBody);
          LOG.log(Level.INFO, "HttpServerBridgeEventHandler onHttpRequest received response: {0}", responseBody);
        } catch (final Exception ex) {
          LOG.log(Level.SEVERE, "Fail to invoke CLR Http Server handler", ex);
          throw new RuntimeException(ex);
        }
      }
    }
  }

  /**
   * Handle failed task.
   */
  public final class FailedTaskHandler implements EventHandler<FailedTask> {
    @Override
    public void onNext(final FailedTask task) throws RuntimeException {
      LOG.log(Level.SEVERE, "FailedTask received, will be handle in CLR handler, if set.");
      if (JobDriver.this.failedTaskHandler == 0) {
        LOG.log(Level.SEVERE, "Failed Task Handler not initialized by CLR, fail for real.");
        throw new RuntimeException("Failed Task Handler not initialized by CLR.");
      }
      try {
        final FailedTaskBridge failedTaskBridge = new FailedTaskBridge(task, activeContextBridgeFactory);
        NativeInterop.clrSystemFailedTaskHandlerOnNext(JobDriver.this.failedTaskHandler, failedTaskBridge,
            JobDriver.this.interopLogger);
      } catch (final Exception ex) {
        LOG.log(Level.SEVERE, "Fail to invoke CLR failed task handler");
        throw new RuntimeException(ex);
      }
    }
  }

  /**
   * Receive notification that the Task is running.
   */
  public final class RunningTaskHandler implements EventHandler<RunningTask> {
    @Override
    public void onNext(final RunningTask task) {
      try (final LoggingScope ls = loggingScopeFactory.taskRunning(task.getId())) {
        if (JobDriver.this.runningTaskHandler == 0) {
          LOG.log(Level.INFO, "RunningTask event received but no CLR handler was bound. Exiting handler.");
        } else {
          LOG.log(Level.INFO, "RunningTask will be handled by CLR handler. Task Id: {0}", task.getId());
          try {
            final RunningTaskBridge runningTaskBridge = new RunningTaskBridge(task, activeContextBridgeFactory);
            NativeInterop.clrSystemRunningTaskHandlerOnNext(JobDriver.this.runningTaskHandler, runningTaskBridge,
                JobDriver.this.interopLogger);
          } catch (final Exception ex) {
            LOG.log(Level.WARNING, "Fail to invoke CLR running task handler");
            throw new RuntimeException(ex);
          }
        }
      }
    }
  }

  /**
   * Receive notification that the Task is running when driver restarted.
   */
  public final class DriverRestartRunningTaskHandler implements EventHandler<RunningTask> {
    @Override
    public void onNext(final RunningTask task) {
      try (final LoggingScope ls = loggingScopeFactory.driverRestartRunningTask(task.getId())) {
        clock.scheduleAlarm(0, new EventHandler<Alarm>() {
          @Override
          public void onNext(final Alarm time) {
            if (JobDriver.this.clrBridgeSetup) {
              if (JobDriver.this.driverRestartRunningTaskHandler != 0) {
                LOG.log(Level.INFO, "CLR driver restart RunningTask handler implemented, now handle it in CLR.");
                NativeInterop.clrSystemDriverRestartRunningTaskHandlerOnNext(
                    JobDriver.this.driverRestartRunningTaskHandler,
                    new RunningTaskBridge(task, activeContextBridgeFactory));
              } else {
                LOG.log(Level.WARNING, "No CLR driver restart RunningTask handler implemented, " +
                    "done with DriverRestartRunningTaskHandler.");
              }
            } else {
              LOG.log(Level.INFO, "Waiting for driver to complete restart process " +
                  "before checking out CLR driver restart RunningTaskHandler...");
              clock.scheduleAlarm(2000, this);
            }
          }
        });
      }
    }
  }

  /**
   * Receive notification that an context is active on Evaluator when the driver restarted.
   */
  public final class DriverRestartActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(final ActiveContext context) {
      try (final LoggingScope ls = loggingScopeFactory.driverRestartActiveContextReceived(context.getId())) {
        JobDriver.this.contexts.put(context.getId(), context);
        LOG.log(Level.INFO, "DriverRestartActiveContextHandler event received: " + context.getId());
        clock.scheduleAlarm(0, new EventHandler<Alarm>() {
          @Override
          public void onNext(final Alarm time) {
            if (JobDriver.this.clrBridgeSetup) {
              if (JobDriver.this.driverRestartActiveContextHandler != 0) {
                LOG.log(Level.INFO, "CLR driver restart ActiveContext handler implemented, now handle it in CLR.");
                NativeInterop.clrSystemDriverRestartActiveContextHandlerOnNext(
                    JobDriver.this.driverRestartActiveContextHandler,
                    activeContextBridgeFactory.getActiveContextBridge(context));
              } else {
                LOG.log(Level.WARNING, "No CLR driver restart ActiveContext handler implemented, " +
                    "done with DriverRestartActiveContextHandler.");
              }
            } else {
              LOG.log(Level.INFO, "Waiting for driver to complete restart process " +
                  "before checking out CLR driver restart DriverRestartActiveContextHandler...");
              clock.scheduleAlarm(2000, this);
            }
          }
        });
      }
    }
  }

  /**
   * Job Driver is ready and the clock is set up: request the evaluators.
   */
  public final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      try (final LoggingScope ls = loggingScopeFactory.driverStart(startTime)) {
        synchronized (JobDriver.this) {

          setupBridge(new DriverStartClrHandlersInitializer(startTime));
          LOG.log(Level.INFO, "Driver Started");
        }
      }
    }
  }


  /**
   * Job driver is restarted after previous crash.
   */
  public final class RestartHandler implements EventHandler<DriverRestarted> {
    @Override
    public void onNext(final DriverRestarted driverRestarted) {
      try (final LoggingScope ls = loggingScopeFactory.driverRestart(driverRestarted.getStartTime())) {
        synchronized (JobDriver.this) {

          JobDriver.this.isRestarted = true;
          setupBridge(new DriverRestartClrHandlersInitializer(driverRestarted));

          LOG.log(Level.INFO, "Driver Restarted and CLR bridge set up.");
        }
      }
    }
  }

  /**
   * Receive notification that driver restart has completed.
   */
  public final class DriverRestartCompletedHandler implements EventHandler<DriverRestartCompleted> {
    @Override
    public void onNext(final DriverRestartCompleted driverRestartCompleted) {
      LOG.log(Level.INFO, "Java DriverRestartCompleted event received at time [{0}]. ",
          driverRestartCompleted.getCompletedTime());
      try (final LoggingScope ls = loggingScopeFactory.driverRestartCompleted(
          driverRestartCompleted.getCompletedTime().getTimeStamp())) {
        if (JobDriver.this.driverRestartCompletedHandler != 0) {
          LOG.log(Level.INFO, "CLR driver restart handler implemented, now handle it in CLR.");

          NativeInterop.clrSystemDriverRestartCompletedHandlerOnNext(
              JobDriver.this.driverRestartCompletedHandler, new DriverRestartCompletedBridge(driverRestartCompleted));
        } else {
          LOG.log(Level.WARNING, "No CLR driver restart handler implemented, done with DriverRestartCompletedHandler.");
        }
      }
    }
  }

  /**
   * Shutting down the job driver: close the evaluators.
   */
  final class StopHandler implements EventHandler<StopTime> {
    @Override
    public void onNext(final StopTime time) {
      LOG.log(Level.INFO, " StopTime: {0}", new Object[]{time});
      try (final LoggingScope ls = loggingScopeFactory.driverStop(time.getTimeStamp())) {
        for (final ActiveContext context : contexts.values()) {
          context.close();
        }
      }
    }
  }

  public final class TaskMessageHandler implements EventHandler<TaskMessage> {
    @Override
    public void onNext(final TaskMessage taskMessage) {
      final String msg = new String(taskMessage.get());
      LOG.log(Level.INFO, "Received TaskMessage: {0} from CLR", msg);
      //try (LoggingScope ls = loggingScopeFactory.taskMessageReceived(new String(msg))) {
      if (JobDriver.this.taskMessageHandler != 0) {
        final TaskMessageBridge taskMessageBridge = new TaskMessageBridge(taskMessage);
        // if CLR implements the task message handler, handle the bytes in CLR handler
        NativeInterop.clrSystemTaskMessageHandlerOnNext(JobDriver.this.taskMessageHandler, taskMessage.get(),
            taskMessageBridge, JobDriver.this.interopLogger);
      }
      //}
    }
  }

  /**
   * Receive notification that the Task has been suspended.
   */
  public final class SuspendedTaskHandler implements EventHandler<SuspendedTask> {
    @Override
    public void onNext(final SuspendedTask task) {
      final String message = "Received notification that task [" + task.getId() + "] has been suspended.";
      LOG.log(Level.INFO, message);
      try (final LoggingScope ls = loggingScopeFactory.taskSuspended(task.getId())) {
        if (JobDriver.this.suspendedTaskHandler != 0) {
          final SuspendedTaskBridge suspendedTaskBridge = new SuspendedTaskBridge(task, activeContextBridgeFactory);
          // if CLR implements the suspended task handler, handle it in CLR
          LOG.log(Level.INFO, "Handling the event of suspended task in CLR bridge.");
          NativeInterop.clrSystemSuspendedTaskHandlerOnNext(JobDriver.this.suspendedTaskHandler, suspendedTaskBridge);
        }
        JobDriver.this.jobMessageObserver.sendMessageToClient(JVM_CODEC.encode(message));
      }
    }
  }

  /**
   * Receive notification that the Evaluator has been shut down.
   */
  public final class CompletedEvaluatorHandler implements EventHandler<CompletedEvaluator> {
    @Override
    public void onNext(final CompletedEvaluator evaluator) {
      LOG.log(Level.INFO, " Completed Evaluator {0}", evaluator.getId());
      try (final LoggingScope ls = loggingScopeFactory.evaluatorCompleted(evaluator.getId())) {
        if (JobDriver.this.completedEvaluatorHandler != 0) {
          final CompletedEvaluatorBridge completedEvaluatorBridge = new CompletedEvaluatorBridge(evaluator);
          // if CLR implements the completed evaluator handler, handle it in CLR
          LOG.log(Level.INFO, "Handling the event of completed evaluator in CLR bridge.");
          NativeInterop.clrSystemCompletedEvaluatorHandlerOnNext(completedEvaluatorHandler, completedEvaluatorBridge);
          allocatedEvaluatorBridges.remove(completedEvaluatorBridge.getId());
        }
      }
    }
  }


  /**
   * Receive notification that the Context had completed.
   * Remove context from the list of active context.
   */
  public final class ClosedContextHandler implements EventHandler<ClosedContext> {
    @Override
    public void onNext(final ClosedContext context) {
      LOG.log(Level.INFO, "Completed Context: {0}", context.getId());
      try (final LoggingScope ls = loggingScopeFactory.closedContext(context.getId())) {
        if (JobDriver.this.closedContextHandler != 0) {
          final ClosedContextBridge closedContextBridge = new ClosedContextBridge(context, activeContextBridgeFactory);
          // if CLR implements the closed context handler, handle it in CLR
          LOG.log(Level.INFO, "Handling the event of closed context in CLR bridge.");
          NativeInterop.clrSystemClosedContextHandlerOnNext(JobDriver.this.closedContextHandler, closedContextBridge);
        }
        synchronized (JobDriver.this) {
          JobDriver.this.contexts.remove(context.getId());
        }
      }
    }
  }


  /**
   * Receive notification that the Context had failed.
   * Remove context from the list of active context and notify the client.
   */
  public final class FailedContextHandler implements EventHandler<FailedContext> {
    @Override
    public void onNext(final FailedContext context) {
      LOG.log(Level.SEVERE, "FailedContext", context);
      try (final LoggingScope ls = loggingScopeFactory.evaluatorFailed(context.getId())) {
        if (JobDriver.this.failedContextHandler != 0) {
          final FailedContextBridge failedContextBridge = new FailedContextBridge(context, activeContextBridgeFactory);
          // if CLR implements the failed context handler, handle it in CLR
          LOG.log(Level.INFO, "Handling the event of failed context in CLR bridge.");
          NativeInterop.clrSystemFailedContextHandlerOnNext(JobDriver.this.failedContextHandler, failedContextBridge);
        }
        synchronized (JobDriver.this) {
          JobDriver.this.contexts.remove(context.getId());
        }
        final Optional<byte[]> err = context.getData();
        if (err.isPresent()) {
          JobDriver.this.jobMessageObserver.sendMessageToClient(err.get());
        }
      }
    }
  }

  /**
   * Receive notification that a ContextMessage has been received.
   */
  public final class ContextMessageHandler implements EventHandler<ContextMessage> {
    @Override
    public void onNext(final ContextMessage message) {
      LOG.log(Level.SEVERE, "Received ContextMessage:", message.get());
      try (final LoggingScope ls = loggingScopeFactory.contextMessageReceived(message.get().toString())) {
        if (JobDriver.this.contextMessageHandler != 0) {
          final ContextMessageBridge contextMessageBridge = new ContextMessageBridge(message);
          // if CLR implements the context message handler, handle it in CLR
          LOG.log(Level.INFO, "Handling the event of context message in CLR bridge.");
          NativeInterop.clrSystemContextMessageHandlerOnNext(JobDriver.this.contextMessageHandler,
              contextMessageBridge);
        }
      }
    }
  }
}
