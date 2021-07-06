package it.uniroma2.query3.ranking;

import it.uniroma2.entity.FirstResult2;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;

public class RankingTrip implements Serializable {

    private static final long serialVersionUID = 1L;

    private final static int k = 5;
    private final List<Trip> ranking;

    public RankingTrip(){
        ranking = new ArrayList<>();
        for (int i = 0; i < k; i ++){
            Trip trip = new Trip("",-1);
            this.add(trip);
        }
    }

    public void add( Trip trip ){
        this.ranking.add(trip);
    }

    public void addTrip( Trip trip ) {
        double distanza = trip.getDistanza();
        int ret = checkIfContain(trip.getTripId());
        if( ret != -1 ){
            //matches update distance
            ranking.remove(ret);
            ranking.add(ret, trip);
            reorder();
            return;
        }
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

    private void reorder() {
        Collections.sort(ranking, Comparator.comparingDouble(Trip::getDistanza));
    }

    private int checkIfContain( String tripId ){
        int ret = -1;
        for( int i=0; i<ranking.size(); i++ ){
            if( ranking.get(i).getTripId().equals(tripId)) ret = i;
        }
        return ret;
    }

    public String getResult(){
        StringBuilder entryResultBld = new StringBuilder();

        for( int i=this.ranking.size()-1; i>=0; i-- ){
            entryResultBld.append(",").append( this.ranking.get(i).getTripId() ).append(",").append( this.ranking.size() - i);
        }
        return entryResultBld.toString();
    }


}
