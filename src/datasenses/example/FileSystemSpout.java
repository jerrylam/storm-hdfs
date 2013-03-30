package datasenses.example;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public abstract class FileSystemSpout extends BaseRichSpout  {

	public static Logger LOG = Logger.getLogger(FileSystemSpout.class);
	
	private SpoutOutputCollector _collector;

	public abstract List<File> listFiles();

	@Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
    }
	
	@Override
	public void nextTuple() {
		for (File file : listFiles())
			_collector.emit(new Values(file
					.getAbsolutePath()), file.getAbsoluteFile());
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("project-path"));
	}
	
	@Override
	public void ack(Object msgId) {
		LOG.info("Complete processing: " + (String) msgId);
	}
	
	@Override
	public void fail(Object msgId) {
		LOG.error("Fail processing: " + (String) msgId);

	}
}
