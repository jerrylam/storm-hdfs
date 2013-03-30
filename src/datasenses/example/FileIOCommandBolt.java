package datasenses.example;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import datasenses.example.command.IOShellCommand;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class FileIOCommandBolt extends BaseRichBolt {
	private final datasenses.example.command.IOShellCommand cmd;
	private final File outputDir;
	
	private OutputCollector _collector;
	
	
	public FileIOCommandBolt(IOShellCommand cmd, File outputDir) throws IOException {
		this.cmd = cmd;
		this.outputDir = outputDir;
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		File inputFile = new File (tuple.getStringByField("output_path"));
		File tmpDir = new File(outputDir, inputFile.getParent());
		File outputFile = new File(tmpDir, cmd.getName()); 
		cmd.create(inputFile, outputFile);
		try {
			cmd.run();
			// anchor the input tuple to track the processing
			_collector.emit(tuple, new Values(outputFile.getAbsolutePath()));
			// ack after it is done
	        _collector.ack(tuple);
		} catch (Exception e) {
			//TODO: report failures to a central system 
			_collector.fail(tuple);
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("output_path"));
	}
}
