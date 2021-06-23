package it.uniroma2.utils;

import java.util.ArrayList;
import java.util.List;

public class LinesBatch {

    private List<String> lines;

    public LinesBatch() {
        this.lines = new ArrayList<String>();
    }

    public List<String> getLines() {
        return lines;
    }

    public void setLines(List<String> lines) {
        this.lines = lines;
    }

    public void addLine(String line){
        if (line != null)
            this.lines.add(line);
    }

    public String getNextLine(){
        if (lines == null || lines.isEmpty())
            return null;
        return lines.remove(0);
    }

}