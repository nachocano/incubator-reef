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
using Org.Apache.REEF.Common.Exceptions;
using Org.Apache.REEF.Driver.Bridge.Events;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Utilities;

namespace Org.Apache.REEF.Driver.Evaluator
{
    /// <summary>
    /// Represents an Evaluator that became unavailable.
    /// </summary>
    public interface IFailedEvaluator : IIdentifiable
    {
        EvaluatorException EvaluatorException { get; set; }

        List<FailedContext> FailedContexts { get; set; }

        Optional<IFailedTask> FailedTask { get; set; }

        [Obsolete("Will be removed after 0.13. Have an instance injected instead.")]
        IEvaluatorRequestor GetEvaluatorRequetor();
    }
}
