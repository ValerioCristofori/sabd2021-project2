package it.uniroma2.query3.ranking;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class RankingTrip implements Serializable {

    private static final long serialVersionUID = 1L;

    Date timestamp;
    List<Trip> ranking;

    public RankingTrip(){
        ranking = new ArrayList<>();
    }

    public void add( Trip trip ){
        this.ranking.add(trip);
    }

    public void addTrip( Trip trip ){
        double distanza = trip.getDistanza();
        if( ranking.size() < 5 )add(trip);
        if(distanza > ranking.get(1).getDistanza()) {
            if(distanza > ranking.get(2).getDistanza()){
                ranking.add(3,trip);
                ranking.remove(0);
            } else {
                ranking.add(2, trip);
                ranking.remove(0);
            }
        } else {
            if(distanza > ranking.get(0).getDistanza()){
                ranking.add(1,trip);
                ranking.remove(0);
            }
        }
    }

    public List<Trip> getRanking() {
        return ranking;
    }

    public void setRanking(List<Trip> ranking) {
        this.ranking = ranking;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }
}
