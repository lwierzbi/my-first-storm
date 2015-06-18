package org.mystorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;


/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        TopologyBuilder builder = new TopologyBuilder();
        WordsSpout testSpout = new WordsSpout();
        builder.setSpout("words", testSpout);
        builder.setBolt("UpperCase", new ToUpperCaseBolt(),1).shuffleGrouping("words");
        builder.setBolt("Reverse", new ReverseOrder(),1).shuffleGrouping("UpperCase");
        
        /* launch topology */
        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(2);
        
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test",conf, builder.createTopology());
        Utils.sleep(10000);
        cluster.killTopology("test");
        cluster.shutdown();
        
    }
}
