package Storm.MyStorm;
import backtype.storm.LocalCluster;
import backtype.storm.testing.TestGlobalCount;
import backtype.storm.testing.TestWordCounter;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.Config;

/**
 * Hello world!
 *
 */

public class App 
{
    public static void main( String[] args )
    {
    	LocalCluster cluster = new LocalCluster();
    	Config conf = new Config();
    	conf.setDebug(true);
    	conf.setNumWorkers(2);  //set the number of worker
    	
    	TopologyBuilder builder = builtBuilder();
    	
    	cluster.submitTopology("test", conf, builder.createTopology());
    }
    
    
    public static TopologyBuilder builtBuilder()
    {
    	TopologyBuilder builder = new TopologyBuilder();
    	builder.setSpout("1", new TestWordSpout(true), 5);
    	builder.setSpout("2", new TestWordSpout(true), 3);
    	builder.setBolt("3", new TestWordCounter(), 3)
    			.fieldsGrouping("1", new Fields("word"))
    			.fieldsGrouping("2", new Fields("word"));
    	builder.setBolt("4", new TestGlobalCount(), 1)
    			.globalGrouping("1");
    	return builder;
    }
    
}

