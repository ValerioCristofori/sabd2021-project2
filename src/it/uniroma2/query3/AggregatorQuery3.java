package it.uniroma2.query3;

import it.uniroma2.entity.FirstResult2;
import it.uniroma2.entity.Result2;
import it.uniroma2.query3.ranking.RankingTrip;
import it.uniroma2.query3.ranking.Trip;
import org.apache.flink.api.common.functions.AggregateFunction;

public class AggregatorQuery3 implements AggregateFunction<Trip, RankingTrip, RankingTrip> {
    @Override
    public RankingTrip createAccumulator() {
        return new RankingTrip();
    }

    @Override
    public RankingTrip add(Trip trip, RankingTrip accumulator) {
        accumulator.add(trip);
        return accumulator;
    }

    @Override
    public RankingTrip getResult(RankingTrip accumulator) {
        return accumulator;
    }

    @Override
    public RankingTrip merge(RankingTrip acc1, RankingTrip acc2) {
        acc2.getRanking().forEach(acc1::addTrip);
        return acc1;
    }
}
