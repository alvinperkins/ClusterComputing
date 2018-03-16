//------------custom wirtableComparable-------------------------------
public static class RedisKey implements WritableComparable<RedisKey> {
 private int lastAccessMonth = 0;
 private Text field = new Text();
 public int getLastAccessMonth() {
 return this.lastAccessMonth;
 }
 public void setLastAccessMonth(int lastAccessMonth) {
 this.lastAccessMonth = lastAccessMonth;
 }
 public Text getField() {
 return this.field;
 }
 public void setField(String field) {
 this.field.set(field);
 }
 public void readFields(DataInput in) throws IOException {
 lastAccessMonth = in.readInt();
 this.field.readFields(in);
 }
 public void write(DataOutput out) throws IOException {
 out.writeInt(lastAccessMonth);
 this.field.write(out);
 }
 public int compareTo(RedisKey rhs) {
 if (this.lastAccessMonth == rhs.getLastAccessMonth()) {
 return this.field.compareTo(rhs.getField());
 } else {
 return this.lastAccessMonth < rhs.getLastAccessMonth() ? -1 : 1;
 }
 }
 public String toString() {
 return this.lastAccessMonth + "\t" + this.field.toString();
 }
 public int hashCode() {
 return toString().hashCode();
 }
}


//-------------------------OutputFormat-----------------------------
public static class RedisLastAccessOutputFormat
 extends OutputFormat<RedisKey, Text> {
 public RecordWriter<RedisKey, Text> getRecordWriter(
 TaskAttemptContext job) throws IOException, InterruptedException {
 return new RedisLastAccessRecordWriter();
 }
 public void checkOutputSpecs(JobContext context) throws IOException,
 InterruptedException {
 }
 public OutputCommitter getOutputCommitter(TaskAttemptContext context)
 throws IOException, InterruptedException {
 return (new NullOutputFormat<Text, Text>()).getOutputCommitter(context);
 }
 public static class RedisLastAccessRecordWriter
 extends RecordWriter<RedisKey, Text> {
 // Code in next section
 }
}

//------------------RecordWRiter--------------------------------
 extends RecordWriter<RedisKey, Text> {
 private HashMap<Integer, Jedis> jedisMap = new HashMap<Integer, Jedis>();
 public RedisLastAccessRecordWriter() {
 // Create a connection to Redis for each host

int i = 0;
 for (String host : MRDPUtils.REDIS_INSTANCES) {
 Jedis jedis = new Jedis(host);
 jedis.connect();
 jedisMap.put(i, jedis);
 jedisMap.put(i + 1, jedis);
 i += 2;
 }
 }
 public void write(RedisKey key, Text value) throws IOException,
 InterruptedException {
 // Get the Jedis instance that this key/value pair will be
 // written to -- (0,1)->0, (2-3)->1, ... , (10-11)->5
 Jedis j = jedisMap.get(key.getLastAccessMonth());
 // Write the key/value pair
 j.hset(MONTH_FROM_INT.get(key.getLastAccessMonth()), key
 .getField().toString(), value.toString());
 }
 public void close(TaskAttemptContext context) throws IOException,
 InterruptedException {
 // For each jedis instance, disconnect it
 for (Jedis jedis : jedisMap.values()) {
 jedis.disconnect();
 }
 }
}


//----------------Mapper---------------------------
public static class RedisLastAccessOutputMapper extends
 Mapper<Object, Text, RedisKey, Text> {
 // This object will format the creation date string into a Date object
 private final static SimpleDateFormat frmt = new SimpleDateFormat(
 "yyyy-MM-dd'T'HH:mm:ss.SSS");
 private RedisKey outkey = new RedisKey();
 private Text outvalue = new Text();
 public void map(Object key, Text value, Context context)
 throws IOException, InterruptedException {
 Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
 .toString());
 String userId = parsed.get("Id");
 String reputation = parsed.get("Reputation");

// Grab the last access date
 String strDate = parsed.get("LastAccessDate");
 // Parse the string into a Calendar object
 Calendar cal = Calendar.getInstance();
 cal.setTime(frmt.parse(strDate));
 // Set our output key and values
 outkey.setLastAccessMonth(cal.get(Calendar.MONTH));
 outkey.setField(userId);
 outvalue.set(reputation);
 context.write(outkey, outvalue);
 }
}

//-------------------Driver---------------------------------
public static void main(String[] args) throws Exception {
 Configuration conf = new Configuration();
 Path inputPath = new Path(args[0]);
 Job job = new Job(conf, "Redis Last Access Output");
 job.setJarByClass(PartitionPruningOutputDriver.class);
 job.setMapperClass(RedisLastAccessOutputMapper.class);
 job.setNumReduceTasks(0);
 job.setInputFormatClass(TextInputFormat.class);
 TextInputFormat.setInputPaths(job, inputPath);
 job.setOutputFormatClass(RedisHashSetOutputFormat.class);
 job.setOutputKeyClass(RedisKey.class);
 job.setOutputValueClass(Text.class);
 int code = job.waitForCompletion(true) ? 0 : 2;
 System.exit(code);
}

//---------------------INputSplit--------------
public static class RedisLastAccessInputSplit
 extends InputSplit implements Writable {
 private String location = null;
 private List<String> hashKeys = new ArrayList<String>();
 public RedisLastAccessInputSplit() {
 // Default constructor for reflection
 }
 public RedisLastAccessInputSplit(String redisHost) {
 this.location = redisHost;
 }
 public void addHashKey(String key) {
 hashKeys.add(key);
 }
 public void removeHashKey(String key) {
 hashKeys.remove(key);
 }
 public List<String> getHashKeys() {
 return hashKeys;
 }
 public void readFields(DataInput in) throws IOException {
 location = in.readUTF();
 int numKeys = in.readInt();

 hashKeys.clear();
 for (int i = 0; i < numKeys; ++i) {
 hashKeys.add(in.readUTF());
 }
 }
 public void write(DataOutput out) throws IOException {
 out.writeUTF(location);
 out.writeInt(hashKeys.size());
 for (String key : hashKeys) {
 out.writeUTF(key);
 }
 }
 public long getLength() throws IOException, InterruptedException {
 return 0;
 }
 public String[] getLocations() throws IOException, InterruptedException {
 return new String[] { location };
 }
}

//---------------------InputFormat---------------------------------
public static class RedisLastAccessInputFormat
 extends InputFormat<RedisKey, Text> {
 public static final String REDIS_SELECTED_MONTHS_CONF =
 "mapred.redilastaccessinputformat.months";
 private static final HashMap<String, Integer> MONTH_FROM_STRING =
 new HashMap<String, Integer>();
 private static final HashMap<String, String> MONTH_TO_INST_MAP =
 new HashMap<String, String>();
 private static final Logger LOG = Logger
.getLogger(RedisLastAccessInputFormat.class);
 static {
 // Initialize month to Redis instance map
 // Initialize month 3 character code to integer
 }
 public static void setRedisLastAccessMonths(Job job, String months) {
 job.getConfiguration().set(REDIS_SELECTED_MONTHS_CONF, months);
 }
 public List<InputSplit> getSplits(JobContext job) throws IOException {
 String months = job.getConfiguration().get(
 REDIS_SELECTED_MONTHS_CONF);
 if (months == null || months.isEmpty()) {
 throw new IOException(REDIS_SELECTED_MONTHS_CONF
 + " is null or empty.");
 }
 // Create input splits from the input months
 HashMap<String, RedisLastAccessInputSplit> instanceToSplitMap =
 new HashMap<String, RedisLastAccessInputSplit>();
 for (String month : months.split(",")) {
 String host = MONTH_TO_INST_MAP.get(month);
 RedisLastAccessInputSplit split = instanceToSplitMap.get(host);
 if (split == null) {
 split = new RedisLastAccessInputSplit(host);
 split.addHashKey(month);
 instanceToSplitMap.put(host, split);
 } else {
 split.addHashKey(month);
 }
 }
 LOG.info("Input splits to process: " +
 instanceToSplitMap.values().size());
 return new ArrayList<InputSplit>(instanceToSplitMap.values());
 }
 public RecordReader<RedisKey, Text> createRecordReader(
 InputSplit split, TaskAttemptContext context)
 throws IOException, InterruptedException {
 return new RedisLastAccessRecordReader();
 }
 public static class RedisLastAccessRecordReader

extends RecordReader<RedisKey, Text> {
 // Code in next section
 }
}


public static class RedisLastAccessRecordReader
 extends RecordReader<RedisKey, Text> {
 private static final Logger LOG = Logger
 .getLogger(RedisLastAccessRecordReader.class);
 private Entry<String, String> currentEntry = null;
 private float processedKVs = 0, totalKVs = 0;
 private int currentHashMonth = 0;
 private Iterator<Entry<String, String>> hashIterator = null;
 private Iterator<String> hashKeys = null;
 private RedisKey key = new RedisKey();
 private String host = null;
 private Text value = new Text();
 public void initialize(InputSplit split, TaskAttemptContext context)
 throws IOException, InterruptedException {
 // Get the host location from the InputSplit
 host = split.getLocations()[0];
 // Get an iterator of all the hash keys we want to read
 hashKeys = ((RedisLastAccessInputSplit) split)
 .getHashKeys().iterator();
 LOG.info("Connecting to " + host);
 }

public boolean nextKeyValue() throws IOException,
 InterruptedException {
 boolean nextHashKey = false;
 do {
 // if this is the first call or the iterator does not have a
 // next
 if (hashIterator == null || !hashIterator.hasNext()) {
 // if we have reached the end of our hash keys, return
 // false
 if (!hashKeys.hasNext()) {
 // ultimate end condition, return false
 return false;
 } else {
 // Otherwise, connect to Redis and get all
 // the name/value pairs for this hash key
 Jedis jedis = new Jedis(host);
 jedis.connect();
 String strKey = hashKeys.next();
 currentHashMonth = MONTH_FROM_STRING.get(strKey);
 hashIterator = jedis.hgetAll(strKey).entrySet()
 .iterator();
 jedis.disconnect();
 }
 }
 // If the key/value map still has values
 if (hashIterator.hasNext()) {
 // Get the current entry and set
 // the Text objects to the entry
 currentEntry = hashIterator.next();
 key.setLastAccessMonth(currentHashMonth);
 key.setField(currentEntry.getKey());
 value.set(currentEntry.getValue());
 } else {
 nextHashKey = true;
 }
 } while (nextHashKey);
 return true;
 }
 ...
}



//----------------------driver--------------------------

public static void main(String[] args) throws Exception {
 Configuration conf = new Configuration();
 String lastAccessMonths = args[0];
 Path outputDir = new Path(args[1]);
 Job job = new Job(conf, "Redis Input");
 job.setJarByClass(PartitionPruningInputDriver.class);
 // Use the identity mapper
 job.setNumReduceTasks(0);
 job.setInputFormatClass(RedisLastAccessInputFormat.class);
 RedisLastAccessInputFormat.setRedisLastAccessMonths(job,
 lastAccessMonths);
 job.setOutputFormatClass(TextOutputFormat.class);
 TextOutputFormat.setOutputPath(job, outputDir);
 job.setOutputKeyClass(RedisKey.class);
 job.setOutputValueClass(Text.class);
 System.exit(job.waitForCompletion(true) ? 0 : 2);
}


