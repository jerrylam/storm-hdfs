package datasenses.example;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class HDFSSpout extends FileSystemSpout {
	private static Logger LOG = LoggerFactory.getLogger(HDFSSpout.class);
	private File projectDir;

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		Object projectPath = conf.get("project_dir");
		if (projectPath != null)
			projectDir = new File((String) projectPath);
		else {
			LOG.error("project path is not provided");
			throw new RuntimeException("Porject Dir is not provided");
		}
	}
	
	@Override
	public List<File> listFiles() {
		List<File> files = Lists.newLinkedList();
		Configuration conf = new Configuration();
		Path inputDirPath = new Path(projectDir.getAbsolutePath());
		LOG.info("Listing files from: " + projectDir.getAbsolutePath());
		FileSystem fs = null;
		try {
			fs = DistributedFileSystem.get(
					URI.create(projectDir.getAbsolutePath()), conf);

			for (FileStatus status : fs.listStatus(inputDirPath)) {
				if (status.isDir())
					continue;
				files.add(new File(status.getPath().toUri().getPath()));
			}
		} catch (IOException e) {
			LOG.error("Unable to locate the projects from " + projectDir, e);
		} finally {
			if (fs != null) {
				try {
					fs.close();
				} catch (IOException e) {
					LOG.error("fail to close hdfs file system: " + e);
				}
			}
		}
		return files;
	}
}