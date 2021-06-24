package it.uniroma2.query.operator;

import com.google.gson.Gson;
import it.uniroma2.utils.Constants;
import it.uniroma2.utils.LinesBatch;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.Map;

public class EntrySpout extends BaseRichSpout {


    public static final String F_DATA 		=	"RowString";
    public static final String F_MSGID		= 	"MSGID";
    public static final String F_TIMESTAMP 	= 	"timestamp";

    private static final long serialVersionUID = 1L;

    private static int SHORT_SLEEP = 10;

    String redisUrl;
    int redisPort;
    int redisTimeout 	= 60000;
    static long msgId 	= 0;
    Jedis jedis;
    SpoutOutputCollector _collector;
    Gson gson;

    public EntrySpout(String redisUrl, int redisPort) {
        this.redisUrl = redisUrl;
        this.redisPort = redisPort;
    }

    @Override
    public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
        jedis = new Jedis(redisUrl, redisPort, redisTimeout);
        _collector = collector;
        gson = new Gson();

    }

    @Override
    public void nextTuple() {

        try {

            String data = jedis.get(Constants.REDIS_DATA);

            while (data == null){

                try {
                    Thread.sleep(SHORT_SLEEP);
                } catch (InterruptedException e) { }

                data = jedis.get(Constants.REDIS_DATA);

            }

            /* Remove file from Redis */
            jedis.del(Constants.REDIS_DATA);

            /* Send data */
            LinesBatch linesBatch = gson.fromJson(data, LinesBatch.class);
            String now = String.valueOf(System.currentTimeMillis());

            for (String row : linesBatch.getLines()) {
                msgId++;
                Values values = new Values();
                values.add(Long.toString(msgId));
                values.add(row);
                values.add(now);
                this._collector.emit(values, msgId);
            }

            jedis.set(Constants.REDIS_CONSUMED, "true");


        } catch (JedisConnectionException e) {
            e.printStackTrace();
            jedis = new Jedis(redisUrl, redisPort, redisTimeout);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields(F_MSGID, F_DATA, F_TIMESTAMP));

    }

    @Override
    public void close() {
        super.close();

        this.jedis.close();
    }
}
