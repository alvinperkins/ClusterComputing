public static void main(String[] args) throws Exception {
 Path userPath = new Path(args[0]);
 Path commentPath = new Path(args[1]);
 Path outputDir = new Path(args[2]);
 String joinType = args[3];
 JobConf conf = new JobConf("CompositeJoin");
 conf.setJarByClass(CompositeJoinDriver.class);
 conf.setMapperClass(CompositeMapper.class);
 conf.setNumReduceTasks(0);
 // Set the input format class to a CompositeInputFormat class.
 // The CompositeInputFormat will parse all of our input files and output
 // records to our mapper.
 conf.setInputFormat(CompositeInputFormat.class);
 // The composite input format join expression will set how the records
 // are going to be read in, and in what input format.
 conf.set("mapred.join.expr", CompositeInputFormat.compose(joinType,
 KeyValueTextInputFormat.class, userPath, commentPath));
 TextOutputFormat.setOutputPath(conf, outputDir);
 conf.setOutputKeyClass(Text.class);
 conf.setOutputValueClass(Text.class);
 RunningJob job = JobClient.runJob(conf);
 while (!job.isComplete()) {
 Thread.sleep(1000);
 }
 System.exit(job.isSuccessful() ? 0 : 1);
}

//-----------Mapper ---------------------------

public static class CompositeMapper extends MapReduceBase implements
 Mapper<Text, TupleWritable, Text, Text> {
 public void map(Text key, TupleWritable value,
 OutputCollector<Text, Text> output,
 Reporter reporter) throws IOException {
 // Get the first two elements in the tuple and output them
 output.collect((Text) value.get(0), (Text) value.get(1));
 }
}


