package it.uniroma2.main;

import au.com.bytecode.opencsv.CSVWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.logging.Logger;

public class CsvWriter {

    private CSVWriter writer;
    private String resultPath = "Results/";
    private Logger log;

    public CsvWriter( String queryName ){
        this.resultPath = this.resultPath + queryName + ".csv";
        try {
            this.writer = new CSVWriter( new FileWriter( this.resultPath, true) );
        } catch (IOException e) {
            log.info("Errore in opening csv writer");
            e.printStackTrace();
        }
    }

    public void appendRow( String row ){
        String[] records = row.split(",");
        this.writer.writeNext(records);
    }

    public void closeWriter(){
        try {
            this.writer.close();
        } catch (IOException e) {
            log.info("Error in closing csv writer");
            e.printStackTrace();
        }
    }
}
