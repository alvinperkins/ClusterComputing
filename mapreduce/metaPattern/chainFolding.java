public static class UserIdCountMapper extends MapReduceBase implements
 Mapper<Object, Text, Text, LongWritable> {
 public static final String RECORDS_COUNTER_NAME = "Records";
 private static final LongWritable ONE = new LongWritable(1);
 private Text outkey = new Text();
 public void map(Object key, Text value,
 OutputCollector<Text, LongWritable> output, Reporter reporter)
 throws IOException {
 Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
 .toString());
 // Get the value for the OwnerUserId attribute
 outkey.set(parsed.get("OwnerUserId"));
 output.collect(outkey, ONE);
 }
}

public static class UserIdReputationEnrichmentMapper extends MapReduceBase
 implements Mapper<Text, LongWritable, Text, LongWritable> {
private Text outkey = new Text();
 private HashMap<String, String> userIdToReputation =
 new HashMap<String, String>();
 public void configure(JobConf job) {
 Path[] files = DistributedCache.getLocalCacheFiles(job);
 // Read all files in the DistributedCache
 for (Path p : files) {
 BufferedReader rdr = new BufferedReader(
 new InputStreamReader(
 new GZIPInputStream(new FileInputStream(
 new File(p.toString())))));
 String line;
 // For each record in the user file
 while ((line = rdr.readLine()) != null) {
 // Get the user ID and reputation
 Map<String, String> parsed = MRDPUtils
 .transformXmlToMap(line);
 // Map the user ID to the reputation
 userIdToReputation.put(parsed.get("Id",
 parsed.get("Reputation"));
 }
 }
 }
 public void map(Text key, LongWritable value,
 OutputCollector<Text, LongWritable> output, Reporter reporter)
 throws IOException {
 String reputation = userIdToReputation.get(key.toString());
 if (reputation != null) {
 outkey.set(value.get() + "\t" + reputation);
 output.collect(outkey, value);
 }
 }
}

//-------------------------reducer

public static class LongSumReducer extends MapReduceBase implements
 Reducer<Text, LongWritable, Text, LongWritable> {
 private LongWritable outvalue = new LongWritable();
 public void reduce(Text key, Iterator<LongWritable> values,
 OutputCollector<Text, LongWritable> output, Reporter reporter)
 throws IOException {
int sum = 0;
 while (values.hasNext()) {
 sum += values.next().get();
 }
 outvalue.set(sum);
 output.collect(key, outvalue);
 }
}

//---------------bining mapper----------------------------------------
public static class UserIdBinningMapper extends MapReduceBase implements
 Mapper<Text, LongWritable, Text, LongWritable> {
 private MultipleOutputs mos = null;
 public void configure(JobConf conf) {
 mos = new MultipleOutputs(conf);
 }
 public void map(Text key, LongWritable value,
 OutputCollector<Text, LongWritable> output, Reporter reporter)
 throws IOException {
 if (Integer.parseInt(key.toString().split("\t")[1]) < 5000) {
 mos.getCollector(MULTIPLE_OUTPUTS_BELOW_5000, reporter)
 .collect(key, value);
 } else {
 mos.getCollector(MULTIPLE_OUTPUTS_ABOVE_5000, reporter)
 .collect(key, value);
 }
 }
 public void close() {
 mos.close();
 }
}

//--------------------driver------------------------------
public static void main(String[] args) throws Exception {
 JobConf conf = new JobConf("ChainMapperReducer");
 conf.setJarByClass(ChainMapperDriver.class);
 Path postInput = new Path(args[0]);
 Path userInput = new Path(args[1]);
 Path outputDir = new Path(args[2]);
 ChainMapper.addMapper(conf, UserIdCountMapper.class,
 LongWritable.class, Text.class, Text.class, LongWritable.class,
 false, new JobConf(false));
 ChainMapper.addMapper(conf, UserIdReputationEnrichmentMapper.class,
 Text.class, LongWritable.class, Text.class, LongWritable.class,
 false, new JobConf(false));
 ChainReducer.setReducer(conf, LongSumReducer.class, Text.class,
 LongWritable.class, Text.class, LongWritable.class, false,
 new JobConf(false));
 ChainReducer.addMapper(conf, UserIdBinningMapper.class, Text.class,
 LongWritable.class, Text.class, LongWritable.class, false,
 new JobConf(false));
 conf.setCombinerClass(LongSumReducer.class);
 conf.setInputFormat(TextInputFormat.class);
 TextInputFormat.setInputPaths(conf, postInput);

 // Configure multiple outputs
 conf.setOutputFormat(NullOutputFormat.class);
 FileOutputFormat.setOutputPath(conf, outputDir);
 MultipleOutputs.addNamedOutput(conf, MULTIPLE_OUTPUTS_ABOVE_5000,
 TextOutputFormat.class, Text.class, LongWritable.class);
 MultipleOutputs.addNamedOutput(conf, MULTIPLE_OUTPUTS_BELOW_5000,
conf.setOutputKeyClass(Text.class);
 conf.setOutputValueClass(LongWritable.class);
 // Add the user files to the DistributedCache
 FileStatus[] userFiles = FileSystem.get(conf).listStatus(userInput);
 for (FileStatus status : userFiles) {
 DistributedCache.addCacheFile(status.getPath().toUri(), conf);
 }
 RunningJob job = JobClient.runJob(conf);
 while (!job.isComplete()) {
 Thread.sleep(5000);
 }
 System.exit(job.isSuccessful() ? 0 : 1);
}


