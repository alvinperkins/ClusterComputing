

public static class CartesianInputFormat extends FileInputFormat {
 public static final String LEFT_INPUT_FORMAT = "cart.left.inputformat";
 public static final String LEFT_INPUT_PATH = "cart.left.path";
 public static final String RIGHT_INPUT_FORMAT = "cart.right.inputformat";
 public static final String RIGHT_INPUT_PATH = "cart.right.path";
 public static void setLeftInputInfo(JobConf job,
 Class<? extends FileInputFormat> inputFormat, String inputPath) {
 job.set(LEFT_INPUT_FORMAT, inputFormat.getCanonicalName());
 job.set(LEFT_INPUT_PATH, inputPath);
 }
 public static void setRightInputInfo(JobConf job,
 Class<? extends FileInputFormat> inputFormat, String inputPath) {
 job.set(RIGHT_INPUT_FORMAT, inputFormat.getCanonicalName());
 job.set(RIGHT_INPUT_PATH, inputPath);
 }
 public InputSplit[] getSplits(JobConf conf, int numSplits)
 throws IOException {
 // Get the input splits from both the left and right data sets
 InputSplit[] leftSplits = getInputSplits(conf,
 conf.get(LEFT_INPUT_FORMAT), conf.get(LEFT_INPUT_PATH),
 numSplits);
 InputSplit[] rightSplits = getInputSplits(conf,
 conf.get(RIGHT_INPUT_FORMAT), conf.get(RIGHT_INPUT_PATH),
 numSplits);
 // Create our CompositeInputSplits, size equal to
 // left.length * right.length
 CompositeInputSplit[] returnSplits =
 new CompositeInputSplit[leftSplits.length *
 rightSplits.length];
 int i = 0;
 // For each of the left input splits
 for (InputSplit left : leftSplits) {
 // For each of the right input splits
 for (InputSplit right : rightSplits) {
 // Create a new composite input split composing of the two
 returnSplits[i] = new CompositeInputSplit(2);
 returnSplits[i].add(left);
 returnSplits[i].add(right);
 ++i;
 }
 }
 // Return the composite splits
 LOG.info("Total splits to process: " + returnSplits.length);
 return returnSplits;
 }
 public RecordReader getRecordReader(InputSplit split, JobConf conf,
 Reporter reporter) throws IOException {
 // Create a new instance of the Cartesian record reader
 return new CartesianRecordReader((CompositeInputSplit) split,
 conf, reporter);
 }
 private InputSplit[] getInputSplits(JobConf conf,
 String inputFormatClass, String inputPath, int numSplits)
 throws ClassNotFoundException, IOException {
 // Create a new instance of the input format
 FileInputFormat inputFormat = (FileInputFormat) ReflectionUtils
 .newInstance(Class.forName(inputFormatClass), conf);
 // Set the input path for the left data set
 inputFormat.setInputPaths(conf, inputPath);
 // Get the left input splits
 return inputFormat.getSplits(conf, numSplits);
 }
}

//----------------Driver code----------------------------------------

public static void main(String[] args) throws IOException,
 InterruptedException, ClassNotFoundException {
 // Configure the join type
 JobConf conf = new JobConf("Cartesian Product");
 conf.setJarByClass(CartesianProduct.class);
 conf.setMapperClass(CartesianMapper.class);
 conf.setNumReduceTasks(0);
 conf.setInputFormat(CartesianInputFormat.class);
 // Configure the input format
 CartesianInputFormat.setLeftInputInfo(conf, TextInputFormat.class, args[0]);
 CartesianInputFormat.setRightInputInfo(conf, TextInputFormat.class, args[0]);
 TextOutputFormat.setOutputPath(conf, new Path(args[1]));
 conf.setOutputKeyClass(Text.class);
 conf.setOutputValueClass(Text.class);
 RunningJob job = JobClient.runJob(conf);
 while (!job.isComplete()) {
 Thread.sleep(1000);
 }
 System.exit(job.isSuccessful() ? 0 : 1);
}


//--------------------record Reader-----------------------
public static class CartesianRecordReader<K1, V1, K2, V2> implements
 RecordReader<Text, Text> {
 // Record readers to get key value pairs
 private RecordReader leftRR = null, rightRR = null;
 // Store configuration to re-create the right record reader
 private FileInputFormat rightFIF;
 private JobConf rightConf;
 private InputSplit rightIS;
 private Reporter rightReporter;
 // Helper variables
 private K1 lkey;
 private V1 lvalue;
 private K2 rkey;
 private V2 rvalue;
 private boolean goToNextLeft = true, alldone = false;

public CartesianRecordReader(CompositeInputSplit split, JobConf conf,
 Reporter reporter) throws IOException {
 this.rightConf = conf;
 this.rightIS = split.get(1);
 this.rightReporter = reporter;
 // Create left record reader
 FileInputFormat leftFIF = (FileInputFormat) ReflectionUtils
 .newInstance(Class.forName(conf
 .get(CartesianInputFormat.LEFT_INPUT_FORMAT)), conf);
 leftRR = leftFIF.getRecordReader(split.get(0), conf, reporter);
 // Create right record reader
 rightFIF = (FileInputFormat) ReflectionUtils.newInstance(Class
 .forName(conf
 .get(CartesianInputFormat.RIGHT_INPUT_FORMAT)), conf);
 rightRR = rightFIF.getRecordReader(rightIS, rightConf, rightReporter);

 // Create key value pairs for parsing
 lkey = (K1) this.leftRR.createKey();
 lvalue = (V1) this.leftRR.createValue();
 rkey = (K2) this.rightRR.createKey();
 rvalue = (V2) this.rightRR.createValue();
 }
 public boolean next(Text key, Text value) throws IOException {
 do {
 // If we are to go to the next left key/value pair
 if (goToNextLeft) {
 // Read the next key value pair, false means no more pairs
 if (!leftRR.next(lkey, lvalue)) {
 // If no more, then this task is nearly finished
 alldone = true;
 break;
 } else {
 // If we aren't done, set the value to the key and set
 // our flags
 key.set(lvalue.toString());
 goToNextLeft = alldone = false;
 // Reset the right record reader
 this.rightRR = this.rightFIF.getRecordReader(
 this.rightIS, this.rightConf,
 this.rightReporter);
 }
 }
 // Read the next key value pair from the right data set
 if (rightRR.next(rkey, rvalue)) {

 // If success, set the value
 value.set(rvalue.toString());
 } else {
 // Otherwise, this right data set is complete
 // and we should go to the next left pair
 goToNextLeft = true;
 }
 // This loop will continue if we finished reading key/value
 // pairs from the right data set
 } while (goToNextLeft);
 // Return true if a key/value pair was read, false otherwise
 return !alldone;
 }
}

//-------------Mapper ---------------------------------------

public static class CartesianMapper extends MapReduceBase implements
 Mapper<Text, Text, Text, Text> {
 private Text outkey = new Text();
 public void map(Text key, Text value,
 OutputCollector<Text, Text> output, Reporter reporter)
 throws IOException {
 // If the two comments are not equal
 if (!key.toString().equals(value.toString())) {
 String[] leftTokens = key.toString().split("\\s");
 String[] rightTokens = value.toString().split("\\s");
 HashSet<String> leftSet = new HashSet<String>(
 Arrays.asList(leftTokens));
 HashSet<String> rightSet = new HashSet<String>(
 Arrays.asList(rightTokens));
 int sameWordCount = 0;
 StringBuilder words = new StringBuilder();
 for (String s : leftSet) {
 if (rightSet.contains(s)) {
 words.append(s + ",");
 ++sameWordCount;
 }
 }
 // If there are at least three words, output

 if (sameWordCount > 2) {
 outkey.set(words + "\t" + key);
 output.collect(outkey, value);
 }
 }
 }
}


