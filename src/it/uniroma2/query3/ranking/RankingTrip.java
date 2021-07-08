package it.uniroma2.query3.ranking;

import java.io.Serializable;
import java.util.*;

public class RankingTrip implements Serializable {

    private static final long serialVersionUID = 1L;

    private final static int k = 5;
    private final List<RankEntity> ranking;

    public RankingTrip(){
        ranking = new ArrayList<>();
        for (int i = 0; i < k; i ++){
            RankEntity rankEntity = new RankEntity("",-1);
            this.add(rankEntity);
        }
    }

    public void add( RankEntity rankEntity){
        this.ranking.add(rankEntity);
    }

    public void addTrip( RankEntity rankEntity) {
        double distanza = rankEntity.getDistanza();
        int ret = checkIfContain(rankEntity.getTripId());
        if( ret != -1 ){
            //matches update distance
            ranking.remove(ret);
            ranking.add(ret, rankEntity);
            reorder();
            return;
        }
        if (distanza >= ranking.get(0).getDistanza()) {

            if (distanza >= ranking.get(1).getDistanza()) {
                if (distanza >= ranking.get(2).getDistanza()) {
                    if (distanza >= ranking.get(3).getDistanza()) {
                        if (distanza >= ranking.get(4).getDistanza()) {
                            ranking.add(5, rankEntity);
                            ranking.remove(0);
                        } else {
                            ranking.add(4, rankEntity);
                            ranking.remove(0);
                        }

                    } else {
                        ranking.add(3, rankEntity);
                        ranking.remove(0);
                    }

                } else {
                    ranking.add(2, rankEntity);
                    ranking.remove(0);
                }
            } else {
                ranking.add(1, rankEntity);
                ranking.remove(0);
            }
        }
    }

    private void reorder() {
        Collections.sort(ranking, Comparator.comparingDouble(RankEntity::getDistanza));
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
            if( !this.ranking.get(i).getTripId().equals("") )
                entryResultBld.append(",").append( this.ranking.get(i).getTripId() ).append(",").append( String.format(Locale.ENGLISH,"%.2f", this.ranking.get(i).getDistanza() ));
            else
                entryResultBld.append(",").append("null").append(",").append("null");
        }
        return entryResultBld.toString();
    }


}
