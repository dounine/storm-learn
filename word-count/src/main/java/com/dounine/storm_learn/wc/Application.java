package com.dounine.storm_learn.wc;


import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class Application {

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("generator",new RandomSentenceSpout(),1);
        builder.setBolt("splitter",new SplitterBolt(),1)
        .shuffleGrouping("generator");
        builder.setBolt("counter",new CounterBolt(),2)
                .fieldsGrouping("splitter",new Fields("world"));
        Config config = new Config();
        config.setDebug(true);
        config.setNumWorkers(1);

        StormSubmitter.submitTopology("world-count",config,builder.createTopology());
    }

}
