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
using Org.Apache.REEF.Utilities;

namespace Org.Apache.REEF.Driver.Evaluator
{
    public sealed class EvaluatorException : Exception
    {
        private readonly string _evaluatorId;
        private readonly Optional<string> _javaStackTrace;

        internal EvaluatorException(string evaluatorId, string message, string javaStackTrace)
            : base(message)
        {
            _evaluatorId = evaluatorId;
            _javaStackTrace = string.IsNullOrWhiteSpace(javaStackTrace)
                ? Optional<string>.Empty()
                : Optional<string>.Of(javaStackTrace);
        }

        internal EvaluatorException(string evaluatorId, string message, Exception inner)
            : base(message, inner)
        {
            _evaluatorId = evaluatorId;
            _javaStackTrace = Optional<string>.Empty();
        }

        public string EvaluatorId
        {
            get { return _evaluatorId; }
        }

        /// <summary>
        /// The Java stack trace of the Evaluator failure.
        /// </summary>
        public Optional<string> JavaStackTrace
        {
            get { return _javaStackTrace; }
        }
    }
}