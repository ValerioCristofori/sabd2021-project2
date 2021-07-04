package it.uniroma2.entity;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class Result2 {

    private static final int k = 3; //top
    private Date timestamp;
    private final List<FirstResult2> mattinaTop3; //ranking top 3 mattina
    private final List<FirstResult2> pomeriggioTop3; //ranking top 3 pomeriggio
    private String mare;

    public Result2(){
        mattinaTop3 = new ArrayList<>();
        pomeriggioTop3 = new ArrayList<>();
        for (int i = 0; i < k; i ++){
            FirstResult2 query2FirstResult = new FirstResult2(-1);
            this.fakeAdd(query2FirstResult);
        }
    }

    public List<FirstResult2> getMattinaTop3(){
        return mattinaTop3;
    }

    public List<FirstResult2> getPomeriggioTop3(){
        return pomeriggioTop3;
    }

    public Date getTimestamp(){
        return timestamp;
    }

    public void setTimestamp(Date timestamp){
        this.timestamp = timestamp;
    }

    public String getMare(){
        return mare;
    }

    public void setMare(String mare){
        this.mare = mare;
    }

    public void fakeAdd(FirstResult2 query2FirstResult){
        mattinaTop3.add(query2FirstResult);
        pomeriggioTop3.add(query2FirstResult);
    }


    public void add(FirstResult2 firstResult){
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(firstResult.getTimestamp());
        int hourOfDay = calendar.get(Calendar.HOUR_OF_DAY);
        if(hourOfDay < 12){
            addMattina(firstResult);
        } else {
            addPomeriggio(firstResult);
        }

    }

    public void addMattina(FirstResult2 firstResult){
        int mattinaFrequentazione = firstResult.getFrequentazione();
        if(mattinaFrequentazione >= mattinaTop3.get(0).getFrequentazione()) {

            if(mattinaFrequentazione >= mattinaTop3.get(1).getFrequentazione()){

                if( mattinaFrequentazione >= mattinaTop3.get(2).getFrequentazione() ){
                    mattinaTop3.add(3,firstResult);
                    mattinaTop3.remove(0);
                }else{
                    mattinaTop3.add(2, firstResult);
                    mattinaTop3.remove(0);
                }

            } else {
                mattinaTop3.add(1, firstResult);
                mattinaTop3.remove(0);
            }
        }
    }

    public void addPomeriggio(FirstResult2 firstResult){
        int pomeriggioFrequentazione = firstResult.getFrequentazione();
        if(pomeriggioFrequentazione >= pomeriggioTop3.get(0).getFrequentazione()) {

            if(pomeriggioFrequentazione >= pomeriggioTop3.get(1).getFrequentazione()){

                if( pomeriggioFrequentazione >= pomeriggioTop3.get(2).getFrequentazione() ){
                    pomeriggioTop3.add(3,firstResult);
                    pomeriggioTop3.remove(0);
                }else{
                    pomeriggioTop3.add(2, firstResult);
                    pomeriggioTop3.remove(0);
                }

            } else {
                pomeriggioTop3.add(1, firstResult);
                pomeriggioTop3.remove(0);
            }
        }
    }


}
