package com.kteq.flink;

import org.apache.flink.util.Collector;

import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

/**
 * Created by mancini on 30/06/2018.
 */
public class TweetsParser implements org.apache.flink.api.common.functions.FlatMapFunction<String, Status> {
    @Override
    public void flatMap(String value, Collector<Status> collector) throws Exception {
        try {
            Status status = TwitterObjectFactory.createStatus(value);
            collector.collect(status);
        } catch (TwitterException e) {
            //we assume that the only case here is a message we do not want to process.
            //it is a test afterall
            //System.err.println(" non status enrty found decoding twitter data: " + s);
        }

    }
}
