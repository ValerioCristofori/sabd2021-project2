package it.uniroma2.utils;

public class Window {

    int[] timeframes;
    int currentIndex;
    int size;
    int estimatedTotal;

    public Window(int size) {

        this.timeframes = new int[size];
        this.size = size;
        this.currentIndex = 0;
        this.estimatedTotal = 0;

        for (int i = 0; i < size; i++)
            timeframes[i] = 0;

    }

    public int moveForward(){

        /* Free the timeframe that is going to go out of the window */
        int lastTimeframeIndex = (currentIndex + 1) % size;

        int value = timeframes[lastTimeframeIndex];
        timeframes[lastTimeframeIndex] = 0;

        estimatedTotal -= value;

        /* Move forward the current index */
        currentIndex = (currentIndex + 1) % size;

        return value;

    }

    public int moveForward(int positions){

        int cumulativeValue = 0;

        for (int i = 0; i < positions; i++){

            cumulativeValue += moveForward();

        }

        return cumulativeValue;
    }

    public void increment(){

        increment(1);

    }

    public void increment(int value){

        timeframes[currentIndex]= timeframes[currentIndex] + value;

        estimatedTotal += value;

    }

    @Override
    public String toString() {

        String s = "[";

        for (int i = 0; i < timeframes.length; i++){

            s += timeframes[i];

            if (i < (timeframes.length - 1))
                s += ", ";

        }

        s += "]";
        return s;

    }

    public int getEstimatedTotal() {
        return estimatedTotal;
    }

    public int computeTotal(){

        int total = 0;
        for (int i = 0; i < timeframes.length; i++)
            total += timeframes[i];
        return total;

    }
}
