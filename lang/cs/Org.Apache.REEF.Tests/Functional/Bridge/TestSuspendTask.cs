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

using System;
using System.Text;
using System.Threading;
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Common.Events;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Common.Tasks.Events;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;

namespace Org.Apache.REEF.Tests.Functional.Bridge
{
    [Collection("FunctionalTests")]
    public sealed class TestSuspendTask : ReefFunctionalTest
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(TestContextStack));

        private const string SuspendMessageFromDriver = "SuspendMessageFromDriver";
        private const string SuspendValidationMessage = "SuspendValidationMessage";
        private const string CompletedValidationMessage = "CompletedValidationmessage";

        /// <summary>
        /// Does a simple test of invoking suspend task with a message from the Driver
        /// and makes sure the target task receives the suspend message.
        /// Uses a shared context between both Tasks to record whether the suspend
        /// message has been received at the Task.
        /// </summary>
        [Fact]
        public void TestSuspendTaskOnLocalRuntime()
        {
            string testFolder = DefaultRuntimeFolder + TestId;
            TestRun(DriverConfigurations(), typeof(SuspendTaskHandlers), 1, "testSuspendTask", "local", testFolder);
            ValidateSuccessForLocalRuntime(1, testFolder: testFolder);
            ValidateMessageSuccessfullyLoggedForDriver(SuspendValidationMessage, testFolder);
            ValidateMessageSuccessfullyLoggedForDriver(CompletedValidationMessage, testFolder);
            CleanUp(testFolder);
        }

        private static IConfiguration DriverConfigurations()
        {
            var helloDriverConfiguration = DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnDriverStarted, GenericType<SuspendTaskHandlers>.Class)
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<SuspendTaskHandlers>.Class)
                .Set(DriverConfiguration.OnContextActive, GenericType<SuspendTaskHandlers>.Class)
                .Set(DriverConfiguration.OnTaskRunning, GenericType<SuspendTaskHandlers>.Class)
                .Set(DriverConfiguration.OnTaskSuspended, GenericType<SuspendTaskHandlers>.Class)
                .Set(DriverConfiguration.OnTaskCompleted, GenericType<SuspendTaskHandlers>.Class)
                .Build();

            return TangFactory.GetTang().NewConfigurationBuilder(helloDriverConfiguration).Build();
        }

        private sealed class SuspendTaskHandlers :
            IObserver<IDriverStarted>,
            IObserver<IAllocatedEvaluator>,
            IObserver<IActiveContext>,
            IObserver<ICompletedTask>,
            IObserver<IRunningTask>,
            IObserver<ISuspendedTask>
        {
            private readonly IEvaluatorRequestor _requestor;
            private bool _firstTask;

            [Inject]
            private SuspendTaskHandlers(IEvaluatorRequestor evaluatorRequestor)
            {
                _requestor = evaluatorRequestor;
                _firstTask = true;
            }

            public void OnNext(IDriverStarted value)
            {
                _requestor.Submit(_requestor.NewBuilder().Build());
            }

            public void OnNext(IActiveContext value)
            {
                // Submit the Task on the first time receiving an active context.
                value.SubmitTask(GetTaskConfiguration());
            }

            public void OnNext(IAllocatedEvaluator value)
            {
                value.SubmitContext(
                    ContextConfiguration.ConfigurationModule
                        .Set(ContextConfiguration.Identifier, "ContextID")
                        .Set(ContextConfiguration.OnContextStart, GenericType<ContextStart>.Class)
                        .Build());
            }

            public void OnNext(ICompletedTask value)
            {
                // Log on task completion to signal a passed test.
                Logger.Log(Level.Warning, CompletedValidationMessage);
                value.ActiveContext.Dispose();
            }

            public void OnNext(ISuspendedTask value)
            {
                if (_firstTask)
                {
                    // Submit a second Task once the first Task has been successfully suspended
                    // on the same context as the first task.
                    Logger.Log(Level.Warning, SuspendValidationMessage);
                    value.ActiveContext.SubmitTask(GetTaskConfiguration());
                }

                // after this we'll get more RunningTask events which have to be ignored
                _firstTask = false;
            }

            public void OnNext(IRunningTask value)
            {
                if (_firstTask)
                {
                    // Suspend the first instance of the Task.
                    value.Suspend(Encoding.UTF8.GetBytes(SuspendMessageFromDriver));
                }
            }

            public void OnError(Exception error)
            {
                throw new NotImplementedException();
            }

            public void OnCompleted()
            {
                throw new NotImplementedException();
            }
        }

        private static IConfiguration GetTaskConfiguration()
        {
            return TaskConfiguration.ConfigurationModule
                .Set(TaskConfiguration.Identifier, "TaskID")
                .Set(TaskConfiguration.Task, GenericType<SuspendTestTask>.Class)
                .Set(TaskConfiguration.OnSuspend, GenericType<SuspendTestTask>.Class)
                .Build();
        }

        private sealed class ContextStart : IObserver<IContextStart>
        {
            private readonly TaskContext _taskContext;

            [Inject]
            private ContextStart(TaskContext taskContext)
            {
                _taskContext = taskContext;
            }

            public void OnNext(IContextStart value)
            {
            }

            public void OnError(Exception error)
            {
                throw new NotImplementedException();
            }

            public void OnCompleted()
            {
                throw new NotImplementedException();
            }
        }

        private sealed class TaskContext
        {
            [Inject]
            private TaskContext()
            {
                TaskSuspended = false;
            }

            public bool TaskSuspended { get; set; }
        }

        /// <summary>
        /// A Task to ensure that an object configured in the second context configuration 
        /// is properly injected.
        /// </summary>
        private sealed class SuspendTestTask : ITask, IObserver<ISuspendEvent>
        {
            private readonly TaskContext _taskContext;
            private readonly CountdownEvent _suspendSignal = new CountdownEvent(1);

            [Inject]
            private SuspendTestTask(TaskContext taskContext)
            {
                _taskContext = taskContext;
            }

            public void Dispose()
            {
            }

            public byte[] Call(byte[] memento)
            {
                if (!_taskContext.TaskSuspended)
                {
                    _suspendSignal.Wait();
                    _taskContext.TaskSuspended = true;
                }

                return null;
            }

            public void OnNext(ISuspendEvent value)
            {
                try
                {
                    Assert.Equal(Encoding.UTF8.GetString(value.Message.Value), SuspendMessageFromDriver);
                }
                finally
                {
                    _suspendSignal.Signal();
                }
            }

            public void OnError(Exception error)
            {
                throw new NotImplementedException();
            }

            public void OnCompleted()
            {
                throw new NotImplementedException();
            }
        }
    }
}