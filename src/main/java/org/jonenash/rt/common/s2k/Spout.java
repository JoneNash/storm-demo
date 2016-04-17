package org.jonenash.rt.common.s2k;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * Created by leidelong on 16/4/13.
 */
public class Spout extends BaseRichSpout {

    private Fields fields = null;
    private SpoutOutputCollector collector ;

    public Spout(Fields fields){
        this.fields = fields;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(this.fields);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {
        Date nowTime=new Date();
        SimpleDateFormat time=new SimpleDateFormat("yyyy MM dd HH mm ss");
        this.collector.emit(new Values("test","spout to kafka -- : "+time.format(nowTime)));
        Utils.sleep(2000);
    }
    @Override
    public void ack(Object o) {

    }

    @Override
    public void fail(Object o) {

    }
}
