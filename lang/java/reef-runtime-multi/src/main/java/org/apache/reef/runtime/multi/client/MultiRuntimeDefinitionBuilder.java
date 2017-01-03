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
package org.apache.reef.runtime.multi.client;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang.StringUtils;
import org.apache.reef.annotations.Unstable;
import org.apache.reef.runtime.multi.utils.avro.AvroMultiRuntimeDefinition;
import org.apache.reef.runtime.multi.utils.avro.AvroRuntimeDefinition;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Builder for multi runtime definition.
 */
@Unstable
public final class MultiRuntimeDefinitionBuilder {
  private Map<String, AvroRuntimeDefinition> runtimes = new HashMap<>();
  private String defaultRuntime;

  private static AvroRuntimeDefinition createRuntimeDefinition(final Configuration configModule,
                                                             final String runtimeName) {
    final Configuration localDriverConfiguration = configModule;
    final AvroConfigurationSerializer serializer = new AvroConfigurationSerializer();
    final String serializedConfig = serializer.toString(localDriverConfiguration);
    return new AvroRuntimeDefinition(runtimeName, serializedConfig);
  }

  /**
   * Adds runtime configuration module to the builder.
   * @param config The configuration module
   * @param runtimeName The name of the runtime
   * @return The builder instance
   */
  public MultiRuntimeDefinitionBuilder addRuntime(final Configuration config, final String runtimeName){
    Validate.notNull(config, "runtime configuration module should not be null");
    Validate.isTrue(StringUtils.isNotBlank(runtimeName),
            "runtimeName should be non empty and non blank string");
    final AvroRuntimeDefinition rd = createRuntimeDefinition(config, runtimeName);
    this.runtimes.put(runtimeName, rd);
    return this;
  }

  /**
   * Sets default runtime name.
   * @param runtimeName The name of the default runtime
   * @return The builder instance
   */
  public MultiRuntimeDefinitionBuilder setDefaultRuntimeName(final String runtimeName){
    Validate.isTrue(StringUtils.isNotBlank(runtimeName),
            "runtimeName should be non empty and non blank string");
    this.defaultRuntime = runtimeName;
    return this;
  }

  /**
   * Builds multi runtime definition.
   * @return The populated definition object
   */
  public AvroMultiRuntimeDefinition build(){
    Validate.isTrue(this.runtimes.size() == 1 || !StringUtils.isEmpty(this.defaultRuntime), "Default runtime " +
            "should be set if more than a single runtime provided");

    if(StringUtils.isEmpty(this.defaultRuntime)){
      // we have single runtime configured, take its name as a default
      this.defaultRuntime = this.runtimes.keySet().iterator().next();
    }

    Validate.isTrue(this.runtimes.containsKey(this.defaultRuntime), "Default runtime should be configured");
    return new AvroMultiRuntimeDefinition(defaultRuntime, new ArrayList<>(this.runtimes.values()));
  }
}
