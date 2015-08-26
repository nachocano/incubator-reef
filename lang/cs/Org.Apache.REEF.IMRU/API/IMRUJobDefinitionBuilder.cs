﻿/**
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

using System;
using Org.Apache.REEF.Network.Group.Driver.Impl;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.IMRU.API
{
    /// <summary>
    /// Use this class to create an IMRU Job Definition.
    /// </summary>
    /// <seealso cref="IMRUJobDefinition" />
    public sealed class IMRUJobDefinitionBuilder
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof (IMRUJobDefinitionBuilder));

        private string _jobName;
        private int _numberOfMappers;
        private int _memoryPerMapper;
        private int _updateTaskMemory;
        private IConfiguration _mapFunctionConfiguration;
        private IConfiguration _mapInputCodecConfiguration;
        private IConfiguration _updateFunctionCodecsConfiguration;
        private IConfiguration _reduceFunctionConfiguration;
        private IConfiguration _updateFunctionConfiguration;
        private IConfiguration _mapOutputPipelineDataConverterConfiguration;
        private IConfiguration _mapInputPipelineDataConverterConfiguration;
        private IConfiguration _partitionedDatasetConfiguration;

        private static readonly IConfiguration EmptyConfiguration =
            TangFactory.GetTang().NewConfigurationBuilder().Build();

        /// <summary>
        /// Constructor
        /// </summary>
        public IMRUJobDefinitionBuilder()
        {
            _mapInputPipelineDataConverterConfiguration = EmptyConfiguration;
            _mapOutputPipelineDataConverterConfiguration = EmptyConfiguration;
            _partitionedDatasetConfiguration = EmptyConfiguration;
            _memoryPerMapper = 512;
            _updateTaskMemory = 512;
        }

        /// <summary>
        /// Set the name of the job.
        /// </summary>
        /// <param name="name">the name of the job</param>
        /// <returns>this</returns>
        public IMRUJobDefinitionBuilder SetJobName(string name)
        {
            _jobName = name;
            return this;
        }

        /// <summary>
        /// Sets configuration of map function
        /// </summary>
        /// <param name="mapFunctionConfiguration">Configuration</param>
        /// <returns>this</returns>
        public IMRUJobDefinitionBuilder SetMapFunctionConfiguration(IConfiguration mapFunctionConfiguration)
        {
            _mapFunctionConfiguration = mapFunctionConfiguration;
            return this;
        }

        /// <summary>
        /// Sets configuration of codec for TMapInput
        /// </summary>
        /// <param name="mapInputCodecConfiguration">Configuration</param>
        /// <returns>this</returns>
        public IMRUJobDefinitionBuilder SetMapInputCodecConfiguration(IConfiguration mapInputCodecConfiguration)
        {
            _mapInputCodecConfiguration = mapInputCodecConfiguration;
            return this;
        }

        /// <summary>
        /// Sets configuration of codecs needed by Update function
        /// </summary>
        /// <param name="updateFunctionCodecsConfiguration">Configuration</param>
        /// <returns>this</returns>
        public IMRUJobDefinitionBuilder SetUpdateFunctionCodecsConfiguration(
            IConfiguration updateFunctionCodecsConfiguration)
        {
            _updateFunctionCodecsConfiguration = updateFunctionCodecsConfiguration;
            return this;
        }

        /// <summary>
        /// Sets configuration of reduce function
        /// </summary>
        /// <param name="reduceFunctionConfiguration">Configuration</param>
        /// <returns>this</returns>
        public IMRUJobDefinitionBuilder SetReduceFunctionConfiguration(IConfiguration reduceFunctionConfiguration)
        {
            _reduceFunctionConfiguration = reduceFunctionConfiguration;
            return this;
        }

        /// <summary>
        /// Sets configuration of update function
        /// </summary>
        /// <param name="updateFunctionConfiguration">Configuration</param>
        /// <returns>this</returns>
        public IMRUJobDefinitionBuilder SetUpdateFunctionConfiguration(IConfiguration updateFunctionConfiguration)
        {
            _updateFunctionConfiguration = updateFunctionConfiguration;
            return this;
        }

        /// <summary>
        /// Sets configuration of PipelineDataConverter for Map output
        /// </summary>
        /// <param name="mapOutputPipelineDataConverterConfiguration">Configuration</param>
        /// <returns>this</returns>
        public IMRUJobDefinitionBuilder SetMapOutputPipelineDataConverterConfiguration(
            IConfiguration mapOutputPipelineDataConverterConfiguration)
        {
            _mapOutputPipelineDataConverterConfiguration = mapOutputPipelineDataConverterConfiguration;
            return this;
        }

        /// <summary>
        /// Sets configuration of PipelineDataConverter for Map Input
        /// </summary>
        /// <param name="mapInputPipelineDataConverterConfiguration">Configuration</param>
        /// <returns>this</returns>
        public IMRUJobDefinitionBuilder SetMapInputPipelineDataConverterConfiguration(
            IConfiguration mapInputPipelineDataConverterConfiguration)
        {
            _mapInputPipelineDataConverterConfiguration = mapInputPipelineDataConverterConfiguration;
            return this;
        }

        /// <summary>
        /// Sets configuration of partitioned dataset
        /// </summary>
        /// <param name="partitionedDatasetConfiguration">Configuration</param>
        /// <returns>this</returns>
        public IMRUJobDefinitionBuilder SetPartitionedDatasetConfiguration(
            IConfiguration partitionedDatasetConfiguration)
        {
            _partitionedDatasetConfiguration = partitionedDatasetConfiguration;
            return this;
        }

        /// <summary>
        /// Sets Number of mappers
        /// </summary>
        /// <param name="numberOfMappers">Number of mappers</param>
        /// <returns>this</returns>
        /// TODO: This is duplicate in a sense that it can be determined 
        /// TODO: automatically from IPartitionedDataset. However, right now 
        /// TODO: GroupComm. instantiated in IMRUDriver needs this parameter 
        /// TODO: in constructor. This will be removed once we remove it from GroupComm. 
        public IMRUJobDefinitionBuilder SetNumberOfMappers(int numberOfMappers)
        {
            _numberOfMappers = numberOfMappers;
            return this;
        }

        /// <summary>
        /// Sets mapper memory
        /// </summary>
        /// <param name="memory">memory in MB</param>
        /// <returns></returns>
        public IMRUJobDefinitionBuilder SetMapperMemory(int memory)
        {
            _memoryPerMapper = memory;
            return this;
        }

        /// <summary>
        /// Set update task memory
        /// </summary>
        /// <param name="memory">memory in MB</param>
        /// <returns></returns>
        public IMRUJobDefinitionBuilder SetUpdateTaskMemory(int memory)
        {
            _updateTaskMemory = memory;
            return this;
        }

        /// <summary>
        /// Instantiate the IMRUJobDefinition.
        /// </summary>
        /// <returns>The IMRUJobDefintion configured.</returns>
        /// <exception cref="NullReferenceException">If any of the required parameters is not set.</exception>
        public IMRUJobDefinition Build()
        {
            if (null == _jobName)
            {
                Exceptions.Throw(new NullReferenceException("Job name cannot be null"),
                    Logger);
            }

            if (null == _mapFunctionConfiguration)
            {
                Exceptions.Throw(new NullReferenceException("Map function configuration cannot be null"), Logger);
            }

            if (null == _mapInputCodecConfiguration)
            {
                Exceptions.Throw(new NullReferenceException("Map input codec configuration cannot be null"), Logger);
            }

            if (null == _updateFunctionCodecsConfiguration)
            {
                Exceptions.Throw(new NullReferenceException("Update function codecs configuration cannot be null"),
                    Logger);
            }

            if (null == _reduceFunctionConfiguration)
            {
                Exceptions.Throw(new NullReferenceException("Reduce function configuration cannot be null"), Logger);
            }

            if (null == _updateFunctionConfiguration)
            {
                Exceptions.Throw(new NullReferenceException("Update function configuration cannot be null"), Logger);
            }

            return new IMRUJobDefinition(
                _mapFunctionConfiguration,
                _mapInputCodecConfiguration,
                _updateFunctionCodecsConfiguration,
                _reduceFunctionConfiguration,
                _updateFunctionConfiguration,
                _mapOutputPipelineDataConverterConfiguration,
                _mapInputPipelineDataConverterConfiguration,
                _partitionedDatasetConfiguration,
                _numberOfMappers,
                _memoryPerMapper,
                _updateTaskMemory,
                _jobName);
        }
    }
}