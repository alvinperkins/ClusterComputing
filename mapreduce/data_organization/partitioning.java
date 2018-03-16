// Set custom partitioner and min last access date
job.setPartitionerClass(LastAccessDatePartitioner.class);
LastAccessDatePartitioner.setMinLastAccessDate(job, 2008);
// Last access dates span between 2008-2011, or 4 years
job.setNumReduceTasks(4);


//--------------Mapper------------------
public static class LastAccessDateMapper extends
 Mapper<Object, Text, IntWritable, Text> {
 // This object will format the creation date string into a Date object
 private final static SimpleDateFormat frmt = new SimpleDateFormat(
 "yyyy-MM-dd'T'HH:mm:ss.SSS");
 private IntWritable outkey = new IntWritable();
 protected void map(Object key, Text value, Context context)
 throws IOException, InterruptedException {

 Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
 .toString());
 // Grab the last access date
 String strDate = parsed.get("LastAccessDate");
 // Parse the string into a Calendar object
 Calendar cal = Calendar.getInstance();
 cal.setTime(frmt.parse(strDate));
 outkey.set(cal.get(Calendar.YEAR));
 // Write out the year with the input value
 context.write(outkey, value);
 }
}

//-----------------Partitioner----------------------------
public static class LastAccessDatePartitioner extends
 Partitioner<IntWritable, Text> implements Configurable {
 private static final String MIN_LAST_ACCESS_DATE_YEAR =
 "min.last.access.date.year";
 private Configuration conf = null;
 private int minLastAccessDateYear = 0;
 public int getPartition(IntWritable key, Text value, int numPartitions) {
 return key.get() - minLastAccessDateYear;
 }
 public Configuration getConf() {
 return conf;
 }
 public void setConf(Configuration conf) {
 this.conf = conf;
 minLastAccessDateYear = conf.getInt(MIN_LAST_ACCESS_DATE_YEAR, 0);
 }

 public static void setMinLastAccessDate(Job job,
 int minLastAccessDateYear) {
 job.getConfiguration().setInt(MIN_LAST_ACCESS_DATE_YEAR,
 minLastAccessDateYear);
 }
}

//------------------reducer---------------------------

public static class ValueReducer extends
 Reducer<IntWritable, Text, Text, NullWritable> {
 protected void reduce(IntWritable key, Iterable<Text> values,
 Context context) throws IOException, InterruptedException {
 for (Text t : values) {
 context.write(t, NullWritable.get());
 }
 }
}


