package com.kteq.flink;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import twitter4j.Status;

/**
 * Created by mancini on 31/08/2018.
 */
class Aggregator extends RichCoFlatMapFunction<Tuple2<String, Status>, Tuple3<String, String, String>,
        Tuple3<String, Set<String>, Statistic>> {

    //state
    private transient ValueState<Set<String>> subscriptionsState;
    private transient ValueState<Integer> countState;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        //setup state
        ValueStateDescriptor<Set<String>> sDescriptor =
                new ValueStateDescriptor<>(
                        "subscriptions", // the state name
                        TypeInformation.of(new TypeHint<Set<String>>() {
                        }) // type information
                );
        subscriptionsState = getRuntimeContext().getState(sDescriptor);

        ValueStateDescriptor<Integer> cDescriptor =
                new ValueStateDescriptor<>(
                        "count", // the state name
                        Integer.class); // type
        countState = getRuntimeContext().getState(cDescriptor);

    }


    @Override
    public void flatMap1(Tuple2<String, Status> value,
            Collector<Tuple3<String, Set<String>, Statistic>> collector) throws Exception {

        //increment (or initialize) the number of seen tweets with the given key
        Integer count = countState.value();
        count = (count == null) ? 1 : (count + 1);
        countState.update(count);

        //"Map1, processing Tweet id: " + value.f1.getId() + ", relative to searchCriteria: " + value.f0;
        Set<String> currentSubscriptions = subscriptionsState.value();

        //emit stat iff there are subscriptions
        if (currentSubscriptions != null && currentSubscriptions.size() > 0)
            collector.collect(new Tuple3<>(value.f0, currentSubscriptions, new Statistic(count)));
     }

    @Override
    public void flatMap2(Tuple3<String, String, String> value,
            Collector<Tuple3<String, Set<String>, Statistic>> collector) throws Exception {

        String subscriber = value.f2;

        //"Map2, processing subscription of " + value.f2 + ", relative to searchCriteria: " + value.f0;

        Set<String> currentSubscriptions = subscriptionsState.value();
        if (currentSubscriptions == null)
            currentSubscriptions = new HashSet<String>();

        if (subscriber != null) {
            if ("-".equals(value.f1)) { //cancel subscription
                currentSubscriptions.remove(subscriber);
            } else {
                if (currentSubscriptions.add(subscriber)) {
                    Integer count = countState.value();
                    count = count == null ? 0 : count;

                    //only the new subscriber should receive the current value.
                    collector.collect(new Tuple3<>(value.f0, Collections.singleton(subscriber), new Statistic(count)));
                }
            }
            subscriptionsState.update(currentSubscriptions);
        }
    }
}
