package it.uniroma2.query3.ranking;


import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class TopKRanking {

    private Comparator<Trip> comparator = null;
    private List<Trip> ranking = null;
    private int topK = 10;

    private static final int NOT_PRESENT = -1;

    public TopKRanking(int k) {
        this.comparator = new RankTripComparator();
        this.ranking = new ArrayList<Trip>();
        this.topK = k;
    }

    /**
     * Update or insert a new RankItem.
     *
     * Returns if the update changed the order of the first observed items.
     *
     * @param item
     * @return
     */
    public boolean update(Trip item) {

        int sizePreUpdate = ranking.size();
        int oldPosition = findIndex(item);

        if (oldPosition != NOT_PRESENT) {
            ranking.remove(item);
        }

        int newPosition = add(item);

        int sizePostUpdate = ranking.size();

        if (newPosition == oldPosition &&
                sizePreUpdate == sizePostUpdate) {

            /* do not notify position changed */
            return false;

        } else if (newPosition > topK - 1) {

            /* do not notify position changed in the lower side of the ranking */
            return false;
        }

        return true;

    }

    public int add(Trip item) {

        int insertionPoint = Collections.binarySearch(ranking, item, comparator);
        ranking.add((insertionPoint > -1) ? insertionPoint : (-insertionPoint) - 1, item);
        insertionPoint = (insertionPoint > -1) ? insertionPoint : (-insertionPoint) - 1;
        return insertionPoint;

    }

    public void remove(Trip item){

        ranking.remove(item);

    }

    private int findIndex(Trip item) {

        for (int i = 0; i < ranking.size(); i++) {
            if (item.equals(ranking.get(i)))
                return i;
        }

        return NOT_PRESENT;

    }

    public boolean containsElement(Trip item) {

        return (findIndex(item) != NOT_PRESENT);

    }

    public RankingTrip getTopK(){

        List<Trip> top = new ArrayList<Trip>();

        if (ranking.isEmpty()){
            RankingTrip topKRanking = new RankingTrip();
            topKRanking.setRanking(top);
            return topKRanking;
        }

        int elems = Math.min(topK, ranking.size());

        for (int i = 0; i < elems; i++){
            top.add(ranking.get(i));
        }

        RankingTrip topKRanking = new RankingTrip();
        topKRanking.setRanking(top);
        return topKRanking;

    }

    @Override
    public String toString() {
        return ranking.toString();
    }

}