/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kteq.flink;

import java.util.Properties;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;

import twitter4j.Status;

public class StreamingJob {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        ParameterTool commandlineParameters = ParameterTool.fromArgs(args);
        String configuarationsFile = commandlineParameters.get("configFile", "application.properties");
        ParameterTool parameters = ParameterTool.fromPropertiesFile(configuarationsFile);


        Properties properties = parameters.getProperties();

        TwitterSource twitterSource = new TwitterSource(properties);


        DataStream<String> tweetsAsStrings = env.addSource(twitterSource);

        //tweetsAsStrings.print();


        DataStream<Status> tweets = tweetsAsStrings.flatMap(new TweetsParser());;

        tweets.print();


        // execute program
        env.execute("Mokabyte Flink Streaming Example");
    }
}