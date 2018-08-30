package com.kteq.flink;

/**
 * Created by mancini on 31/08/2018.
 *
 * oggetto utilizzato per raccogliere le statistice circa
 * un dato search criteria
 * al momento contiene solo un contatore.
 */
public class Statistic {

    private int count;

    public Statistic(int count) {
        this.count = count;
    }

    public int getCount() {
        return count;
    }

    @Override
    public String toString() {
        return "Statistic{" +
                "count=" + count +
                '}';
    }
}
