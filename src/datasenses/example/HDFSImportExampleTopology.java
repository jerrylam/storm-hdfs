package datasenses.example;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.io.File;
import java.util.Map;

import datasenses.example.command.IOShellCommand;

/**
 * This is a basic example of using HDFS as Spout for Storm topology
 */
public class HDFSExampleTopology {
	
	private static class ExampleCommand extends IOShellCommand {

		private String command;
		@Override
		public String getName() {
			return "EXAMPLE";
		}

		@Override
		public String create(File inputFile, File outputFile) {
			// process input file and produce outputfile
			return command;
		}

		@Override
		protected String get() {
			// TODO Auto-generated method stub
			return command;
		}
		
	}
    
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("hdfs", new HDFSSpout(), 1);        
        builder.setBolt("bolt", new FileIOCommandBolt(new ExampleCommand(), new File("/tmp/example")), 10)
                .shuffleGrouping("hdfs");
                
        Config conf = new Config();
        conf.setDebug(true);
        
        if(args!=null && args.length > 0) {
            conf.setNumWorkers(3);
            
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
        
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(10000);
            cluster.killTopology("test");
            cluster.shutdown();    
        }
    }
}
