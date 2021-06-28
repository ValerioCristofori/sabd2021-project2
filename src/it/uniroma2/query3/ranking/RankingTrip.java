package it.uniroma2.query3.ranking;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class RankingTrip implements Serializable {

    private static final long serialVersionUID = 1L;

    List<Trip> ranking;

    public RankingTrip(){
        ranking = new ArrayList<>();
    }

    public List<Trip> getRanking() {
        return ranking;
    }

    public void setRanking(List<Trip> ranking) {
        this.ranking = ranking;
    }
}
