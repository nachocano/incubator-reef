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
using System.Collections.Generic;
using System.Runtime.Serialization;
using Org.Apache.REEF.Common.Exceptions;
using Org.Apache.REEF.Driver.Bridge.Clr2java;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Driver.Bridge.Events
{
    [DataContract]
    internal class FailedEvaluator : IFailedEvaluator
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(FailedEvaluator));

        public FailedEvaluator(IFailedEvaluatorClr2Java clr2Java)
        {
            InstanceId = Guid.NewGuid().ToString("N");
            FailedEvaluatorClr2Java = clr2Java;
            EvaluatorRequestorClr2Java = FailedEvaluatorClr2Java.GetEvaluatorRequestor();
            Id = FailedEvaluatorClr2Java.GetId();
        }

        [DataMember]
        public string InstanceId { get; set; }

        public string Id { get; private set; }

        public EvaluatorException EvaluatorException { get; set; }

        public List<FailedContext> FailedContexts { get; set; }

        public Optional<IFailedTask> FailedTask { get; set; }

        [DataMember]
        private IFailedEvaluatorClr2Java FailedEvaluatorClr2Java { get; set; }

        [DataMember]
        private IEvaluatorRequestorClr2Java EvaluatorRequestorClr2Java { get; set; }

        public IEvaluatorRequestor GetEvaluatorRequetor()
        {
            if (EvaluatorRequestorClr2Java == null)
            {
                Exceptions.Throw(new InvalidOperationException("EvaluatorRequestorClr2Java not initialized."), LOGGER);
            }
            return new EvaluatorRequestor(EvaluatorRequestorClr2Java);
        }
    }
}
