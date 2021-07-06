package it.uniroma2.query3;

import it.uniroma2.query3.ranking.RankingTrip;
import it.uniroma2.query3.ranking.Trip;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class AggregatorQuery3 implements AggregateFunction<Tuple3<String,Long,Double>, RankingTrip, String> {
    @Override
    public RankingTrip createAccumulator() {
        return new RankingTrip();
    }

    @Override
    public RankingTrip add(Tuple3<String,Long,Double> tuple3, RankingTrip ranking) {
        ranking.addTrip(new Trip(tuple3.f0,tuple3.f2) );
        return ranking;
    }

    @Override
    public String getResult(RankingTrip ranking) {
        return ranking.getResult();
    }

    @Override
    public RankingTrip merge(RankingTrip acc1, RankingTrip acc2) {
        return null;
    }
}
