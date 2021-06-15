package it.uniroma2.query1.spout;

import au.com.bytecode.opencsv.CSVReader;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;


import java.io.FileReader;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class EntrySpout extends BaseRichSpout {

    private final String fileName = "data/prj2_dataset_imported.csv";
    private final char separator = ',';
    private CSVReader reader;
    private AtomicLong linesRead;
    private SpoutOutputCollector _collector;

    public EntrySpout(){
        linesRead=new AtomicLong(0);
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

        _collector = spoutOutputCollector;
        try {
            reader = new CSVReader(new FileReader(fileName), separator);
            // read and ignore the header if one exists
            reader.readNext();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void nextTuple() {
        try {
            String[] line = reader.readNext();
            if (line != null) {
                long id=linesRead.incrementAndGet();
                _collector.emit(new Values(line),id);
            }
            else
                System.out.println("Finished reading file, "+linesRead.get()+" lines read");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
        System.err.println("Failed tuple with id "+id);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        try {
            CSVReader reader = new CSVReader(new FileReader(fileName), separator);
            // read csv header to get field info
            String[] fields = reader.readNext();

            System.out.println("DECLARING OUTPUT FIELDS");
            for (String a : fields)
                System.out.println(a);

            declarer.declare(new Fields(Arrays.asList(fields)));

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}

