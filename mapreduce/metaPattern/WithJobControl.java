public static final String AVERAGE_CALC_GROUP = "AverageCalculation";
public static final String MULTIPLE_OUTPUTS_ABOVE_NAME = "aboveavg";
public static final String MULTIPLE_OUTPUTS_BELOW_NAME = "belowavg";
public static void main(String[] args) throws Exception {
 Configuration conf = new Configuration();

 Path postInput = new Path(args[0]);
 Path userInput = new Path(args[1]);
 Path countingOutput = new Path(args[3] + "_count");
 Path binningOutputRoot = new Path(args[3] + "_bins");
 Path binningOutputBelow = new Path(binningOutputRoot + "/"
 + JobChainingDriver.MULTIPLE_OUTPUTS_BELOW_NAME);
 Path binningOutputAbove = new Path(binningOutputRoot + "/"
 + JobChainingDriver.MULTIPLE_OUTPUTS_ABOVE_NAME);
 Path belowAverageRepOutput = new Path(args[2]);
 Path aboveAverageRepOutput = new Path(args[3]);
 Job countingJob = getCountingJob(conf, postInput, countingOutput);
 int code = 1;
 if (countingJob.waitForCompletion(true)) {
 ControlledJob binningControlledJob = new ControlledJob(
 getBinningJobConf(countingJob, conf, countingOutput,
 userInput, binningOutputRoot));
 ControlledJob belowAvgControlledJob = new ControlledJob(
 getAverageJobConf(conf, binningOutputBelow,
 belowAverageRepOutput));
 belowAvgControlledJob.addDependingJob(binningControlledJob);
 ControlledJob aboveAvgControlledJob = new ControlledJob(
 getAverageJobConf(conf, binningOutputAbove,
 aboveAverageRepOutput));
 aboveAvgControlledJob.addDependingJob(binningControlledJob);
 JobControl jc = new JobControl("AverageReputation");
 jc.addJob(binningControlledJob);
 jc.addJob(belowAvgControlledJob);
 jc.addJob(aboveAvgControlledJob);
 jc.run();
 code = jc.getFailedJobList().size() == 0 ? 0 : 1;
 }
 FileSystem fs = FileSystem.get(conf);
 fs.delete(countingOutput, true);
 fs.delete(binningOutputRoot, true);
 System.exit(code);
}

//----------------helper-----------------------------------
public static Job getCountingJob(Configuration conf, Path postInput,
 Path outputDirIntermediate) throws IOException {
 // Setup first job to counter user posts
 Job countingJob = new Job(conf, "JobChaining-Counting");
 countingJob.setJarByClass(JobChainingDriver.class);
 // Set our mapper and reducer, we can use the API's long sum reducer for
 // a combiner!
 countingJob.setMapperClass(UserIdCountMapper.class);
 countingJob.setCombinerClass(LongSumReducer.class);
 countingJob.setReducerClass(UserIdSumReducer.class);
 countingJob.setOutputKeyClass(Text.class);
 countingJob.setOutputValueClass(LongWritable.class);
 countingJob.setInputFormatClass(TextInputFormat.class);
 TextInputFormat.addInputPath(countingJob, postInput);
 countingJob.setOutputFormatClass(TextOutputFormat.class);
 TextOutputFormat.setOutputPath(countingJob, outputDirIntermediate);
 return countingJob;
}
public static Configuration getBinningJobConf(Job countingJob,
 Configuration conf, Path jobchainOutdir, Path userInput,
 Path binningOutput) throws IOException {
 // Calculate the average posts per user by getting counter values
 double numRecords = (double) countingJob
 .getCounters()
 .findCounter(JobChainingDriver.AVERAGE_CALC_GROUP,
 UserIdCountMapper.RECORDS_COUNTER_NAME).getValue();
 double numUsers = (double) countingJob
 .getCounters()
 .findCounter(JobChainingDriver.AVERAGE_CALC_GROUP,
 UserIdSumReducer.USERS_COUNTER_NAME).getValue();
 double averagePostsPerUser = numRecords / numUsers;
 // Setup binning job
 Job binningJob = new Job(conf, "JobChaining-Binning");
 binningJob.setJarByClass(JobChainingDriver.class);
 // Set mapper and the average posts per user
 binningJob.setMapperClass(UserIdBinningMapper.class);
 UserIdBinningMapper.setAveragePostsPerUser(binningJob,
 averagePostsPerUser);
 binningJob.setNumReduceTasks(0);
 binningJob.setInputFormatClass(TextInputFormat.class);
TextInputFormat.addInputPath(binningJob, jobchainOutdir);
 // Add two named outputs for below/above average
 MultipleOutputs.addNamedOutput(binningJob,
 JobChainingDriver.MULTIPLE_OUTPUTS_BELOW_NAME,
 TextOutputFormat.class, Text.class, Text.class);
 MultipleOutputs.addNamedOutput(binningJob,
 JobChainingDriver.MULTIPLE_OUTPUTS_ABOVE_NAME,
 TextOutputFormat.class, Text.class, Text.class);
 MultipleOutputs.setCountersEnabled(binningJob, true);
 // Configure multiple outputs
 conf.setOutputFormat(NullOutputFormat.class);
 FileOutputFormat.setOutputPath(conf, outputDir);
 MultipleOutputs.addNamedOutput(conf, MULTIPLE_OUTPUTS_ABOVE_5000,
 TextOutputFormat.class, Text.class, LongWritable.class);
 MultipleOutputs.addNamedOutput(conf, MULTIPLE_OUTPUTS_BELOW_5000,
 TextOutputFormat.class, Text.class, LongWritable.class);
 // Add the user files to the DistributedCache
 FileStatus[] userFiles = FileSystem.get(conf).listStatus(userInput);
 for (FileStatus status : userFiles) {
 DistributedCache.addCacheFile(status.getPath().toUri(),
 binningJob.getConfiguration());
 }
 // Execute job and grab exit code
 return binningJob.getConfiguration();
}
public static Configuration getAverageJobConf(Configuration conf,
 Path averageOutputDir, Path outputDir) throws IOException {
 Job averageJob = new Job(conf, "ParallelJobs");
 averageJob.setJarByClass(ParallelJobs.class);
 averageJob.setMapperClass(AverageReputationMapper.class);
 averageJob.setReducerClass(AverageReputationReducer.class);
 averageJob.setOutputKeyClass(Text.class);
 averageJob.setOutputValueClass(DoubleWritable.class);
 averageJob.setInputFormatClass(TextInputFormat.class);
 TextInputFormat.addInputPath(averageJob, averageOutputDir);
 averageJob.setOutputFormatClass(TextOutputFormat.class);
 TextOutputFormat.setOutputPath(averageJob, outputDir);

 // Execute job and grab exit code
 return averageJob.getConfiguration();
}


