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
using System.IO;
using Org.Apache.REEF.Client.Common;
using Org.Apache.REEF.Common.Jar;
using Org.Apache.REEF.Driver;
using Xunit;

namespace Org.Apache.REEF.Tests.Utility
{
    [Collection("FunctionalTests")]
    public class TestDriverConfigGenerator
    {
        public TestDriverConfigGenerator()
        {
            var resourceHelper = new ResourceHelper(typeof(IJobSubmissionResult).Assembly);
            var fileName = resourceHelper.GetString(ResourceHelper.DriverJarFullName);
            if (!File.Exists(fileName))
            {
                File.WriteAllBytes(fileName, 
                    resourceHelper.GetBytes(ResourceHelper.FileResources[ResourceHelper.DriverJarFullName]));
            }
        }

        [Fact]
        public void TestGeneratingFullDriverConfigFile()
        {
            DriverConfigurationSettings driverConfigurationSettings = new DriverConfigurationSettings()
            {
                DriverMemory = 1024,
                DriverIdentifier = "juliaDriverId",
                SubmissionDirectory = "reefClrBridgeTmp/job_" + Guid.NewGuid().ToString("N").Substring(0, 4),
                IncludingHttpServer = true,
                IncludingNameServer = true,
                ClrFolder = ".",
                JarFileFolder = ".\\"
            };

            DriverConfigGenerator.DriverConfigurationBuilder(driverConfigurationSettings);
        }

        [Fact]
        public void TestGeneratingDriverConfigFileWithoutHttp()
        {
            DriverConfigurationSettings driverConfigurationSettings = new DriverConfigurationSettings()
            {
                DriverMemory = 1024,
                DriverIdentifier = "juliaDriverId",
                SubmissionDirectory = "reefClrBridgeTmp/job_" + Guid.NewGuid().ToString("N").Substring(0, 4),
                IncludingHttpServer = false,
                IncludingNameServer = true,
                ClrFolder = ".",
                JarFileFolder = ".\\"
            };

            DriverConfigGenerator.DriverConfigurationBuilder(driverConfigurationSettings);
        }

        [Fact]
        public void TestGeneratingDriverConfigFileWithoutNameServer()
        {
            DriverConfigurationSettings driverConfigurationSettings = new DriverConfigurationSettings()
            {
                DriverMemory = 1024,
                DriverIdentifier = "juliaDriverId",
                SubmissionDirectory = "reefClrBridgeTmp/job_" + Guid.NewGuid().ToString("N").Substring(0, 4),
                IncludingHttpServer = true,
                IncludingNameServer = false,
                ClrFolder = ".",
                JarFileFolder = ".\\"
            };

            DriverConfigGenerator.DriverConfigurationBuilder(driverConfigurationSettings);
        }

        [Fact]
        public void TestGeneratingDriverConfigFileDriverOnly()
        {
            DriverConfigurationSettings driverConfigurationSettings = new DriverConfigurationSettings()
            {
                DriverMemory = 1024,
                DriverIdentifier = "juliaDriverId",
                SubmissionDirectory = "reefClrBridgeTmp/job_" + Guid.NewGuid().ToString("N").Substring(0, 4),
                IncludingHttpServer = false,
                IncludingNameServer = false,
                ClrFolder = ".",
                JarFileFolder = ".\\"
            };

            DriverConfigGenerator.DriverConfigurationBuilder(driverConfigurationSettings);
        }
    }
}
