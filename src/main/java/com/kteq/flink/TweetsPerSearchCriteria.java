package com.kteq.flink;

import java.util.Arrays;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import twitter4j.Status;

/**
 * Created by mancini on 24/05/2018.
 */
class TweetsPerSearchCriteria implements FlatMapFunction<Status, Tuple2<String, Status>> {
    @Override
    public void flatMap(Status status, Collector<Tuple2<String, Status>> collector) throws Exception {
        if (status != null) {
            Arrays.stream(status.getHashtagEntities()).forEach(
                    hte -> collector.collect(new Tuple2<>("#" + hte.getText(), status))
            );

            Arrays.stream(status.getUserMentionEntities()).forEach(
                    ume -> collector.collect(new Tuple2<>("@" + ume.getText(), status))
            );
        }

    }
}
