package it.uniroma2.query3.ranking;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class RankingTrip implements Serializable {

    private static final long serialVersionUID = 1L;

    private final static int k = 5;
    List<Trip> ranking;

    public RankingTrip(){
        ranking = new ArrayList<>();
    }

    public void add( Trip trip ){
        this.ranking.add(trip);
    }

    public void addTrip( Trip trip ) {
        double distanza = trip.getDistanza();
        if (ranking.size() < 5) add(trip);
        if (distanza >= ranking.get(0).getDistanza()) {

            if (distanza >= ranking.get(1).getDistanza()) {
                if (distanza >= ranking.get(2).getDistanza()) {
                    if (distanza >= ranking.get(3).getDistanza()) {
                        if (distanza >= ranking.get(4).getDistanza()) {
                            ranking.add(5, trip);
                            ranking.remove(0);
                        } else {
                            ranking.add(4, trip);
                            ranking.remove(0);
                        }

                    } else {
                        ranking.add(3, trip);
                        ranking.remove(0);
                    }

                } else {
                    ranking.add(2, trip);
                    ranking.remove(0);
                }
            } else {
                ranking.add(1, trip);
                ranking.remove(0);
            }
        }
    }

    public String getResult(){
        StringBuilder entryResultBld = new StringBuilder();

        for( int i=this.ranking.size()-1; i>=0; i-- ){
            entryResultBld.append(",").append( this.ranking.get(i).getTripId() ).append(",").append( this.ranking.size() - i);
        }
        return entryResultBld.toString();
    }


}
