package it.uniroma2.entity;

public class Result2 {

    private Date date;
    private List<FirstResult2> am3;
    private List<FirstResult2> pm3;
    private String mare;

    public Result2(){
        am3 = new ArrayList<>();
        pm3 = new ArrayList<>();
        for (int i = 0; i < 3; i ++){
            FirstResult2 query2FirstResult = new FirstResult2(-1);
            this.forceAdd(query2FirstResult);
        }
    }

    public List<FirstResult2> getAm3(){
        return am3;
    }

    public List<FirstResult2> getPm3(){
        return pm3;
    }

    public Date getDate(){
        return date;
    }

    public String getMare(){
        return mare;
    }

    public void setMare(String mare){
        this.mare = mare;
    }

    public void forceAdd(FirstResult2 query2FirstResult){
        am3.add(query2FirstResult);
        pm3.add(query2FirstResult);
    }

    public void amAdd(FirstResult2 firstResult){
        int amFrequentazione = firstResult.getFrequentazione();
        if(amFrequentazione > am3.get(1).getFrequentazione()) {
            if(amFrequentazione > am3.get(2).getFrequentazione()){
                am3.add(3,firstResult);
                am3.remove(0);
            } else {
                am3.add(2, firstResult);
                am3.remove(0);
            }
        } else {
            if(amFrequentazione > am3.get(0).getFrequentazione()){
                am3.add(1,firstResult);
                am3.remove(0);
            }
        }
    }

    public void pmAdd(FirstResult2 firstResult){
        int pmFrequentazione = firstResult.getFrequentazione();
        if(pmFrequentazione > pm3.get(1).getFrequentazione()) {
            if(pmFrequentazione > pm3.get(2).getFrequentazione()){
                pm3.add(3,firstResult);
                pm3.remove(0);
            } else {
                pm3.add(2, firstResult);
                pm3.remove(0);
            }
        } else {
            if(pmFrequentazione > pm3.get(0).getFrequentazione()){
                pm3.add(1,firstResult);
                pm3.remove(0);
            }
        }
    }

    public void add(FirstResult2 firstResult){
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(firstResult.getDate());
        int hourOfDay = calendar.get(Calendar.HOUR_OF_DAY);
        if(hourOfDay < 12){
            amAdd(firstResult);
        } else {
            pmAdd(firstResult);
        }

    }

}
