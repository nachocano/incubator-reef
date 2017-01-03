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
package org.apache.reef.runtime.common.launch;

import com.google.protobuf.ByteString;
import org.apache.reef.proto.ReefServiceProtos;
import org.apache.reef.runtime.common.launch.parameters.ErrorHandlerRID;
import org.apache.reef.runtime.common.launch.parameters.LaunchID;
import org.apache.reef.runtime.common.utils.ExceptionCodec;
import org.apache.reef.runtime.common.utils.RemoteManager;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * The error handler REEF registers with Wake.
 */
public final class REEFErrorHandler implements EventHandler<Throwable>, AutoCloseable {

  private static final Logger LOG = Logger.getLogger(REEFErrorHandler.class.getName());
  private static final String CLASS_NAME = REEFErrorHandler.class.getCanonicalName();

  // This class is used as the ErrorHandler in the RemoteManager. Hence, we need an InjectionFuture here.
  private final InjectionFuture<RemoteManager> remoteManager;
  private final String launchID;
  private final String errorHandlerRID;
  private final ExceptionCodec exceptionCodec;

  @Inject
  REEFErrorHandler(
      @Parameter(ErrorHandlerRID.class) final String errorHandlerRID,
      @Parameter(LaunchID.class) final String launchID,
      final InjectionFuture<RemoteManager> remoteManager,
      final ExceptionCodec exceptionCodec) {

    this.errorHandlerRID = errorHandlerRID;
    this.remoteManager = remoteManager;
    this.launchID = launchID;
    this.exceptionCodec = exceptionCodec;
  }

  @Override
  @SuppressWarnings("checkstyle:illegalcatch")
  public void onNext(final Throwable ex) {

    LOG.log(Level.SEVERE, "Uncaught exception.", ex);

    if (this.errorHandlerRID.equals(ErrorHandlerRID.NONE)) {
      LOG.log(Level.SEVERE, "Caught an exception from Wake we cannot send upstream because there is no upstream");
      return;
    }

    try {

      final EventHandler<ReefServiceProtos.RuntimeErrorProto> runtimeErrorHandler =
          this.remoteManager.get().getHandler(this.errorHandlerRID, ReefServiceProtos.RuntimeErrorProto.class);

      final ReefServiceProtos.RuntimeErrorProto message =
          ReefServiceProtos.RuntimeErrorProto.newBuilder()
              .setName("reef")
              .setIdentifier(this.launchID)
              .setMessage(ex.getMessage())
              .setException(ByteString.copyFrom(this.exceptionCodec.toBytes(ex)))
              .build();

      runtimeErrorHandler.onNext(message);
      LOG.log(Level.INFO, "Successfully sent the error upstream: {0}", ex.toString());

    } catch (final Throwable t) {
      LOG.log(Level.SEVERE, "Unable to send the error upstream", t);
    }
  }

  @SuppressWarnings("checkstyle:illegalcatch")
  public void close() {

    LOG.entering(CLASS_NAME, "close");

    try {
      this.remoteManager.get().close();
    } catch (final Throwable ex) {
      LOG.log(Level.SEVERE, "Unable to close the remote manager", ex);
    }

    LOG.exiting(CLASS_NAME, "close");
  }

  @Override
  public String toString() {
    return String.format(
        "REEFErrorHandler: { remoteManager:{%s}, launchID:%s, errorHandlerRID:%s }",
        this.remoteManager.get(), this.launchID, this.errorHandlerRID);
  }
}
