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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

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


        DataStream<Status> tweets = tweetsAsStrings.flatMap(new TweetsParser());
        //tweets.print();


        DataStream<Tuple2<String, Status>>
                classifiedTweets = tweets.flatMap(new TweetsPerSearchCriteria());

        //tweets.print();

//        //Stampa Tag, id_tweet, lista hashtags e mentions.
//        classifiedTweets.map(new MapFunction<Tuple2<String, Status>, String>() {
//            @Override
//            public String map(Tuple2<String, Status> value) throws Exception {
//                return value.f0 + " -> " + value.f1.getId() + " " +
//                        "[" +
//                        Stream.concat(
//                                Arrays.stream(value.f1.getHashtagEntities()).map(e -> "#" + e.getText()),
//                                Arrays.stream(value.f1.getUserMentionEntities()).map(e -> "@" + e.getText())
//                        ).collect(Collectors.joining(",")) +
//                        "]";
//            }
//        }).print();

        //Source che prende i dati dal topic "topic" della coda Apache Kafka configurata come da "properties"
        //durante lo sviluppo e' torppo laborioso gestire la Kafka attiva quindi usiamo una lista di
        //stringhe statiche.
        //FlinkKafkaConsumer011<String> kafkaConsumer
        //        = new FlinkKafkaConsumer011<>("topic", new SimpleStringSchema(), parameters.getProperties());

        List<String> subscriptions = Arrays.asList("Alberto,+,#HR", "Francesca,+,#Job", "Alberto,+,@DemGovs");
        DataStream<String> configurationsAsStrings = env.fromCollection(subscriptions);
        DataStream<Tuple3<String, String, String>> configurations = configurationsAsStrings.map(new SubsriptionsParser());


        ConnectedStreams<Tuple2<String, Status>, Tuple3<String, String, String>>
                tweetsAndConfigurations = classifiedTweets.connect(configurations);


        //processa i due stream accoppiati e stampa un messaggio identificativo
        //in entrambe le map.
        tweetsAndConfigurations.map(new CoMapFunction<Tuple2<String,Status>, Tuple3<String,String,String>, String>() {


            @Override
            public String map1(Tuple2<String, Status> value) throws Exception {
                return "Map1, processing Tweet id: " + value.f1.getId() + ", relative to searchCriteria: " + value.f0;
            }

            @Override
            public String map2(Tuple3<String, String, String> value) throws Exception {
                return "Map2, processing subscription od " + value.f2 + ", relative to searchCriteria: " + value.f0;
            }
        }).print();






        // execute program
        env.execute("Mokabyte Flink Streaming Example");
    }
}
