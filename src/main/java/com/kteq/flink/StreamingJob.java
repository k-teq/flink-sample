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

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
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

        //legge i Tweet come stringhe
        TwitterSource twitterSource = new TwitterSource(properties);
        DataStream<String> tweetsAsStrings = env.addSource(twitterSource);

        //Trasforma le stringhe in twttt4j.Status
        DataStream<Status> tweets = tweetsAsStrings.flatMap(new TweetsParser());

        //organizza i tweet per search criteria
        DataStream<Tuple2<String, Status>>
                classifiedTweets = tweets.flatMap(new TweetsPerSearchCriteria());

        //legge da kafka le 'subscruptions'
        FlinkKafkaConsumer010<String> kafkaConsumer
                = new FlinkKafkaConsumer010<>(Arrays.asList(parameters.get("topic")),
                new SimpleStringSchema(), parameters.getProperties());
        DataStream<String> configurationsAsStrings = env.addSource(kafkaConsumer);

        //preprocessa le stringhe provenienti da kafka
        DataStream<Tuple3<String, String, String>> configurations = configurationsAsStrings.map(new SubsriptionsParser());


        //connette i due stream (tweets,subscriptions)
        ConnectedStreams<Tuple2<String, Status>, Tuple3<String, String, String>>
                tweetsAndConfigurations = classifiedTweets.connect(configurations);


        //partiziona lo  stream (connected) usando un key extractor.
        ConnectedStreams<Tuple2<String, Status>, Tuple3<String, String, String>> keyedTweetsAndConfigurations =
                tweetsAndConfigurations.keyBy(
                        new KeySelector<Tuple2<String, Status>, String>() {
                            @Override
                            public String getKey(Tuple2<String, Status> value) throws Exception {
                                return value.f0;
                            }
                        },
                        new KeySelector<Tuple3<String, String, String>, String>() {
                            @Override
                            public String getKey(Tuple3<String, String, String> value) throws Exception {
                                return value.f0;
                            }
                        }
                );


        //processa i due stream accoppiati
        //ritorna la statistica, la key  e tutte le subscriptions per successive elaborazioni
        SingleOutputStreamOperator<Tuple3<String, Set<String>, Statistic>>
                stats = keyedTweetsAndConfigurations.flatMap(new Aggregator());


        //salva le statistiche su jbase
        stats.writeUsingOutputFormat( new HBaseOutputFormat( properties.getProperty("tableName") ) );



        // execute program
        env.execute("Mokabyte Flink Streaming Example");
    }

}
