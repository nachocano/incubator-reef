﻿// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.IMRU.Examples.PipelinedBroadcastReduce;
using Org.Apache.REEF.IMRU.OnREEF.Driver;
using Org.Apache.REEF.IMRU.OnREEF.Parameters;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Xunit;
using TestSenderMapFunction = Org.Apache.REEF.IMRU.Examples.PipelinedBroadcastReduce.PipelinedBroadcastAndReduceWithFaultTolerant.SenderMapFunctionFT;

namespace Org.Apache.REEF.Tests.Functional.IMRU
{
    [Collection("FunctionalTests")]
    public sealed class TestFailMapperEvaluatorsOnInit : TestFailMapperEvaluators
    {
        /// <summary>
        /// This test fails two evaluators during task initialize stage on each retry except last. 
        /// Job is retried until success.
        /// </summary>
        [Fact]
        public override void TestFailedMapperOnLocalRuntime()
        {
            int chunkSize = 2;
            int dims = 100;
            int iterations = 200;
            int mapperMemory = 5120;
            int updateTaskMemory = 5120;
            int numTasks = 9;
            string testFolder = DefaultRuntimeFolder + TestId;
            TestBroadCastAndReduce(false,
                numTasks,
                chunkSize,
                dims,
                iterations,
                mapperMemory,
                updateTaskMemory,
                NumberOfRetry,
                testFolder);
            string[] lines = ReadLogFile(DriverStdout, "driver", testFolder, 360);
            var completedTaskCount = GetMessageCount(lines, "Received ICompletedTask");
            var failedEvaluatorCount = GetMessageCount(lines, FailedEvaluatorMessage);
            var failedTaskCount = GetMessageCount(lines, FailedTaskMessage);
            var jobSuccess = GetMessageCount(lines, IMRUDriver<int[], int[], int[], int[]>.DoneActionPrefix);

            // In each retry, there are 2 failed evaluators.
            // There will be no failed task.
            // Rest of the tasks should be canceled and send completed task event to the driver. 
            Assert.Equal(NumberOfRetry * 2, failedEvaluatorCount);
            Assert.Equal(0, failedTaskCount);
            Assert.True(((NumberOfRetry + 1) * numTasks) - failedEvaluatorCount >= completedTaskCount);
            Assert.True((NumberOfRetry * numTasks) - failedEvaluatorCount < completedTaskCount);

            // eventually job succeeds
            Assert.Equal(1, jobSuccess);
            CleanUp(testFolder);
        }

        protected override IConfiguration BuildMapperFunctionConfig()
        {
            var c = IMRUMapConfiguration<int[], int[]>.ConfigurationModule
                .Set(IMRUMapConfiguration<int[], int[]>.MapFunction,
                    GenericType<TestSenderMapFunction>.Class)
                .Build();

            return TangFactory.GetTang().NewConfigurationBuilder(c)
                .BindSetEntry<PipelinedBroadcastAndReduceWithFaultTolerant.TaskIdsToFail, string>(GenericType<PipelinedBroadcastAndReduceWithFaultTolerant.TaskIdsToFail>.Class, "IMRUMap-RandomInputPartition-2-")
                .BindSetEntry<PipelinedBroadcastAndReduceWithFaultTolerant.TaskIdsToFail, string>(GenericType<PipelinedBroadcastAndReduceWithFaultTolerant.TaskIdsToFail>.Class, "IMRUMap-RandomInputPartition-3-")
                .BindIntNamedParam<PipelinedBroadcastAndReduceWithFaultTolerant.FailureType>(PipelinedBroadcastAndReduceWithFaultTolerant.FailureType.EvaluatorFailureDuringTaskInitialization.ToString())
                .BindNamedParameter(typeof(MaxRetryNumberInRecovery), NumberOfRetry.ToString())
                .BindNamedParameter(typeof(PipelinedBroadcastAndReduceWithFaultTolerant.TotalNumberOfForcedFailures), NumberOfRetry.ToString())
                .Build();
        }
    }
}