# An example of using storm with HDFS

storm-hdfs demonstrates one way of processing files resided in HDFS using storm. The spout is implemented to read a directory in HDFS and process all files within the directory by emitting the file path to the subsequent bolts in the workflow.
