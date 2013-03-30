package datasenses.example;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
	
	private Set<File> workingSet = new HashSet<File>();
	
	// every 30 minutes, check for new files to process
	private final static int WAITING_TIME_MS = 30 * 60 * 1000;
	
	@Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        //TODO: populate the working set if it is restarted from a storage
    }
	
	@Override
	public void nextTuple() {
		try {
			// check if new files exist
			for (File file : listFiles()) {
				if (!workingSet.contains(file)) {
					_collector.emit(new Values(file
							.getAbsolutePath()), file.getAbsoluteFile());
					workingSet.add(file);
				}
			}
			Thread.sleep(WAITING_TIME_MS);
		} catch (InterruptedException e) {
			LOG.error(e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("project-path"));
	}
	
	@Override
	public void ack(Object msgId) {
		LOG.info("Complete processing: " + (String) msgId);
		//TODO: persist the msgId to keep track
	}
	
	@Override
	public void fail(Object msgId) {
		// remove the file from the working set
		workingSet.remove(new File((String)msgId));
		LOG.error("Fail processing: " + (String) msgId);

	}
}
