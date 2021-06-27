package it.uniroma2.query3.ranking;

import java.util.Comparator;

public class RankTripComparator implements Comparator<Trip> {
    @Override
    public int compare(Trip o1, Trip o2) {
        double distance1 = o1.getDistanza();
        double distance2 = o2.getDistanza();

        return (int) -(distance1 - distance2);
    }
}
