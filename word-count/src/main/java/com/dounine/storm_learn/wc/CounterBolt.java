package com.dounine.storm_learn.wc;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class CounterBolt extends BaseBasicBolt {

    private static final Map<String,Integer> WORLD_COUNT = new HashMap<>();

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String world = input.getStringByField("world");
        Integer count = WORLD_COUNT.get(world);
        if(count==null){
            count = 1;
        }else{
            count++;
        }
        WORLD_COUNT.put(world,count);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("count"));
    }
}
