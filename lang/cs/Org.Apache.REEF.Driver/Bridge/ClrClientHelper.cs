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
using System.Diagnostics;
using System.IO;
using System.Linq;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Driver.Bridge
{
    public class ClrClientHelper
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(ClrClientHelper));

        [Obsolete("please use ReefClient API")]
        public static void Run(HashSet<string> appDlls, IConfiguration driverBridgeConfig,
            DriverSubmissionSettings driverSubmissionSettings, string reefJar = Constants.JavaBridgeJarFileName,
            string runCommand = "run.cmd", string clrFolder = ".", string className = Constants.BridgeLaunchClass)
        {
            using (LOGGER.LogFunction("ClrHandlerHelper::Run"))
            {
                if (driverSubmissionSettings.Submit)
                {
                    ClrHandlerHelper.CopyDllsToAppDirectory(appDlls);
                    UpdateJarFileWithAssemblies(reefJar);
                }

                using (LOGGER.LogScope("ClrHandlerHelper::serialize driverBridgeConfig to clrRuntimeConfigFile"))
                {
                    var clrRuntimeConfigFile = Path.Combine(clrFolder, Constants.DriverBridgeConfiguration);
                    new AvroConfigurationSerializer().ToFile(driverBridgeConfig, clrRuntimeConfigFile);
                    LOGGER.Log(Level.Info, "CLR driver bridge configurations written to " + clrRuntimeConfigFile);
                }

                var startInfo = new ProcessStartInfo();
                if (driverSubmissionSettings.RunOnYarn)
                {
                    startInfo.FileName = runCommand;
                    startInfo.Arguments = className + " " + clrFolder +
                                          driverSubmissionSettings.ToComamndLineArguments();
                }
                else
                {
                    startInfo.FileName = GetJavaBinary();
                    var loggingPrefix = string.Empty;
                    if (driverSubmissionSettings.JavaLogLevel == JavaLoggingSetting.VERBOSE_TO_CLR)
                    {
                        loggingPrefix = Constants.JavaToCLRLoggingConfig + " ";
                    }
                    else if (driverSubmissionSettings.JavaLogLevel == JavaLoggingSetting.VERBOSE)
                    {
                        loggingPrefix = Constants.JavaVerboseLoggingConfig + " ";
                    }
                    startInfo.Arguments = loggingPrefix + @"-classpath " + reefJar + " " + Constants.BridgeLaunchClass +
                                          " " + clrFolder + " " + driverSubmissionSettings.ToComamndLineArguments();
                }
                startInfo.RedirectStandardOutput = true;
                startInfo.UseShellExecute = false;
                startInfo.CreateNoWindow = false;
                LOGGER.Log(Level.Info, "Executing\r\n" + startInfo.FileName + "\r\n" + startInfo.Arguments);
                using (var process = Process.Start(startInfo))
                {
                    process.WaitForExit();
                }
            }
        }

        [Obsolete("please use ReefClient API")]
        public static void UpdateJarFileWithAssemblies(string reefJar)
        {
            using (LOGGER.LogFunction("ClrHandlerHelper::UpdateJarFileWithAssemblies"))
            {
                var assembliesList = ClrHandlerHelper.GetAssembliesListForReefDriverApp();
                if (!File.Exists(reefJar))
                {
                    throw new InvalidOperationException("cannot find reef jar file: " + reefJar);
                }
                var startInfo = new ProcessStartInfo
                {
                    FileName = GetJarBinary(),
                    Arguments = @"uf " + reefJar + " " + assembliesList,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false,
                    CreateNoWindow = true
                };

                LOGGER.Log(Level.Info, "updating jar file with \r\n" + startInfo.FileName + "\r\n" + startInfo.Arguments);
                using (var process = Process.Start(startInfo))
                {
                    var outReader = process.StandardOutput;
                    var errorReader = process.StandardError;
                    var output = outReader.ReadToEnd();
                    var error = errorReader.ReadToEnd();
                    process.WaitForExit();
                    if (process.ExitCode != 0)
                    {
                        throw new InvalidOperationException("Failed to update jar file with stdout :" + output +
                                                            "and stderr:" + error);
                    }
                }
                LOGGER.Log(Level.Info, "jar file updated.");
            }
        }

        public static void ExtractConfigfileFromJar(string reefJar, IList<string> configFiles, string dropFolder)
        {
            var configFileNames = string.Join(" ", configFiles.ToArray());
            var startInfo = new ProcessStartInfo
            {
                FileName = GetJarBinary(),
                Arguments = @"xf " + reefJar + " " + configFileNames,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            };

            LOGGER.Log(Level.Info,
                "extracting files from jar file with \r\n" + startInfo.FileName + "\r\n" + startInfo.Arguments);
            using (var process = Process.Start(startInfo))
            {
                var outReader = process.StandardOutput;
                var errorReader = process.StandardError;
                var output = outReader.ReadToEnd();
                var error = errorReader.ReadToEnd();
                process.WaitForExit();
                if (process.ExitCode != 0)
                {
                    throw new InvalidOperationException("Failed to extract files from jar file with stdout :" + output +
                                                        "and stderr:" + error);
                }
            }
            LOGGER.Log(Level.Info, "files are extracted.");
        }

        private static string GetJarBinary()
        {
            var javaHome = Environment.GetEnvironmentVariable("JAVA_HOME");
            if (string.IsNullOrWhiteSpace(javaHome))
            {
                LOGGER.Log(Level.Info, "JAVA_HOME not set. Please set JAVA_HOME environment variable first. Exiting...");
                Environment.Exit(1);
            }
            return Path.Combine(javaHome, "bin", "jar.exe");
        }

        private static string GetJavaBinary()
        {
            var javaHome = Environment.GetEnvironmentVariable("JAVA_HOME");
            if (string.IsNullOrWhiteSpace(javaHome))
            {
                LOGGER.Log(Level.Info, "JAVA_HOME not set. Please set JAVA_HOME environment variable first. Exiting...");
                Environment.Exit(1);
            }
            return Path.Combine(javaHome, "bin", "java.exe");
        }
    }
}