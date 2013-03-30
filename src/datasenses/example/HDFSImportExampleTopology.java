package datasenses.example;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import java.io.File;

import datasenses.example.command.IOShellCommand;

/**
 * This is a basic example of using HDFS as Spout for Storm topology
 */
public class HDFSImportExampleTopology {
	
	private static class HDSFImportCommand extends IOShellCommand {

		private String command;
		@Override
		public String getName() {
			return "IMPORT";
		}

		@Override
		public String create(File inputFile, File outputFile) {
			// process input file and produce outputfile
			command = "hadoop fs -get " + inputFile.getAbsolutePath() + " " + outputFile.getAbsolutePath(); 
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
        builder.setBolt("import", new FileIOCommandBolt(new HDSFImportCommand(), new File("/tmp/import")), 10)
                .shuffleGrouping("hdfs");
                
        Config conf = new Config();
        conf.setDebug(true);
        
        // never timeout
        conf.setMessageTimeoutSecs(Integer.MAX_VALUE);
        
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