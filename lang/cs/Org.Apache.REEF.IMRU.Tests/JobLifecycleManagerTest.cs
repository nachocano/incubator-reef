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
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.IMRU.OnREEF.Driver;
using Xunit;

namespace Org.Apache.REEF.IMRU.Tests
{
    public class JobLifecycleManagerTest
    {
        [Fact]
        [Trait("Description", "Verify that JobCancelled event is sent when cancellation signal is detected.")]
        public void JobLifeCycleMangerSendsJobCancelledEvent()
        {
            string expectedMessage = "cancelled";
            var observer = JobLifeCycleMangerEventTest(
                detector: new SampleJobCancelledDetector(true, expectedMessage))
                .FirstOrDefault();

            AssertCancelEvent(observer, true, expectedMessage);
        }

        [Fact]
        [Trait("Description", "Verify that JobCancelled Event can be sent to all subscribers in case of multiply observers.")]
        public void JobLifeCycleMangerSendsJobCancelledEventToMultiplyObservers()
        {
            string expectedMessage = "cancelled";
            var observers = JobLifeCycleMangerEventTest(
                detector: new SampleJobCancelledDetector(true, expectedMessage));

            foreach (var observer in observers)
            {
                AssertCancelEvent(observer, true, expectedMessage);
            }
        }

        [Fact]
        [Trait("Description", "Verify that IsCancelled check is performed with specified period.")]
        public void JobLifeCycleMangerChecksDetectorPeriodically()
        {
            string expectedMessage = "cancelled";
            int isCancelledCheckCounter = 0;

            var observer = JobLifeCycleMangerEventTest(
                detector: new SampleJobCancelledDetector(true, expectedMessage, testAction: () => { isCancelledCheckCounter++; }),
                signalCheckPeriodSec: 1,
                waitForEventPeriodSec: 6)
                .FirstOrDefault();

            Assert.True(isCancelledCheckCounter >= 5, "Expected 5+ IsCancelled checks in 6 sec (check interval = 1 sec). Actual check counter: " + isCancelledCheckCounter);
            AssertCancelEvent(observer, true, expectedMessage);
        }

        [Fact]
        [Trait("Description", "Verify that JobLifecycle manager does not sent any cancellation events if signal is not generated.")]
        public void JobLifeCycleMangerNoSignalDoesNotSendEvent()
        {
            var observer = JobLifeCycleMangerEventTest(
                detector: new SampleJobCancelledDetector(false))
                .FirstOrDefault();

            AssertCancelEvent(observer, false);
        }

        [Fact]
        [Trait("Description", "Verify that no cancellation event is sent if configured detector is null.")]
        public void JobLifeCycleMangerDetectorNullDoesNotSendEvent()
        {
            var observer = JobLifeCycleMangerEventTest(
                detector: null)
                .FirstOrDefault();

            AssertCancelEvent(observer, false);
        }

        [Fact]
        [Trait("Description", "Verify that cancellation checks are not performed if there are no observers.")]
        public void JobLifeCycleMangerNoObserversDoesNotCheckForSignal()
        {
            int isCancelledCheckCounter = 0;

            var observer = JobLifeCycleMangerEventTest(
                detector: new SampleJobCancelledDetector(true, "cancelled", testAction: () => { isCancelledCheckCounter++; }),
                subscribeObserver: false,
                signalCheckPeriodSec: 1,
                waitForEventPeriodSec: 6)
                .FirstOrDefault();

            Assert.True(isCancelledCheckCounter == 0, "Expected no checks for cancellation if there are no subscribers. Actual check counter: " + isCancelledCheckCounter);
            AssertCancelEvent(observer, false);
        }

        [Fact]
        [Trait("Description", "Verify that manager stops checking for cancellation signal after all observers unsubscribed.")]
        public void JobLifeCycleMangerNoCancellationChecksAfterAllObserversUnsubscribed()
        {
            int isCancelledCheckCounter = 0;

            const int waitForEventPeriodSec = 6;

            var observer = JobLifeCycleMangerEventTest(
                detector: new SampleJobCancelledDetector(true, "cancelled", testAction: () => { isCancelledCheckCounter++; }),
                subscribeObserver: false,
                signalCheckPeriodSec: 1,
                waitForEventPeriodSec: waitForEventPeriodSec)
                .FirstOrDefault();

            Assert.True(isCancelledCheckCounter == 0, "Expected no checks for cancellation if there are no subscribers. Actual check counter: " + isCancelledCheckCounter);

            // subscribe observer - checks should start incrementing
            observer.Subscribe();
            Thread.Sleep(waitForEventPeriodSec * 1000);
            Assert.True(isCancelledCheckCounter > 0, "Expected checks for cancellation after new subscritpion added. Actual check counter: " + isCancelledCheckCounter);

            // unsubscribe and verify that checks for cancellation are not incrementing anymore
            observer.UnSubscribe();
            var counterAfterUnsubscribe = isCancelledCheckCounter;
            Thread.Sleep(waitForEventPeriodSec * 1000);
            Assert.True(isCancelledCheckCounter == counterAfterUnsubscribe, "Expected no checks for cancellation after all subscribers unsubscribed. Actual check counter: " + isCancelledCheckCounter + " expected Counter to stay at: " + counterAfterUnsubscribe);
        }

        private IEnumerable<TestObserver> JobLifeCycleMangerEventTest(
            IJobCancelledDetector detector,
            bool subscribeObserver = true,
            int observerCount = 1,
            int signalCheckPeriodSec = 1,
            int waitForEventPeriodSec = 2)
        {
            var manager = Activator.CreateInstance(
                typeof(JobLifeCycleManager),
                BindingFlags.NonPublic | BindingFlags.Instance,
                null,
                new object[] { detector, signalCheckPeriodSec },
                null,
                null) as JobLifeCycleManager;

            var observers = Enumerable.Range(1, observerCount)
                .Select(_ => new TestObserver(manager, subscribeObserver))
                .ToList();

            Thread.Sleep(waitForEventPeriodSec * 1000);

            return observers;
        }

        private void AssertCancelEvent(TestObserver observer, bool expectedEvent, string expectedMessage = null)
        {
            if (expectedEvent)
            {
                Assert.NotNull(observer.LastEvent);
                Assert.Same(expectedMessage, observer.LastEvent.Message);
            }
            else
            {
                Assert.Null(observer.LastEvent);
            }
        }

        private IDriverStarted NewStartedEvent()
        {
            // event is not really used by the driver, so can use null here
            return null;
        }

        /// <summary>
        /// Test helper class to provide predefined cancel signal for testing
        /// </summary>
        private class SampleJobCancelledDetector : IJobCancelledDetector
        {
            private bool _isCancelledResponse;
            private string _cancellationMessage;
            private Action _actionOnIsCancelledCall;

            internal SampleJobCancelledDetector(bool isCancelledResponse, string expectedMessage = null, Action testAction = null)
            {
                _isCancelledResponse = isCancelledResponse;
                _cancellationMessage = expectedMessage;
                _actionOnIsCancelledCall = testAction;
            }

            public bool IsJobCancelled(out string cancellationMessage)
            {
                if (_actionOnIsCancelledCall != null)
                {
                    _actionOnIsCancelledCall();
                }

                cancellationMessage = this._cancellationMessage;
                return _isCancelledResponse;
            }
        }

        /// <summary>
        /// Test helper class to record JobCancelled events from lifecycle manager
        /// </summary>
        private class TestObserver : IObserver<IJobCancelled> 
        {
            internal IJobCancelled LastEvent { get; private set; }
            internal IObservable<IJobCancelled> source { get; private set; }

            internal IDisposable subscription { get; private set; }

            internal TestObserver(IObservable<IJobCancelled> eventSource, bool autoSubscribe)
            {
                source = eventSource;
                if (autoSubscribe)
                {
                    Subscribe();
                }
            }

            public void OnNext(IJobCancelled value)
            {
                LastEvent = value;
            }

            public void OnError(Exception error)
            {
            }

            public void OnCompleted()
            {
            }

            public void Subscribe()
            {
                subscription = source.Subscribe(this);
            }

            public void UnSubscribe()
            {
                subscription.Dispose();
            }
        }
    }
}
