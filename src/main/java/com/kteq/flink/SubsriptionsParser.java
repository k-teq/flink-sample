package com.kteq.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Created by mancini on 24/05/2018.
 */
class SubsriptionsParser implements MapFunction<String, Tuple3<String, String, String>> {
    @Override
    public Tuple3<String, String, String> map(String s) throws Exception {
        String[] parts = s.split(",");
        return new Tuple3<>(parts[2], parts[1], parts[0]); //key, add/remove, user
    }
}
