public static class TaggedText implements WritableComparable<TaggedText> {
 private String tag = "";
 private Text text = new Text();
 public TaggedText() { }
 public void setTag(String tag) {
 this.tag = tag;
 }
 public String getTag() {
 return tag;
 }
 public void setText(Text text) {
 this.text.set(text);
 }

 public void setText(String text) {

 this.text.set(text);
 }
 public Text getText() {
 return text;
 }
 public void readFields(DataInput in) throws IOException {
 tag = in.readUTF();
 text.readFields(in);
 }
 public void write(DataOutput out) throws IOException {
 out.writeUTF(tag);
 text.write(out);
 }
 public int compareTo(TaggedText obj) {
 int compare = tag.compareTo(obj.getTag());
 if (compare == 0) {
 return text.compareTo(obj.getText());
 } else {
 return compare;
 }
 }

 public String toString() {
 return tag.toString() + ":" + text.toString();
 }
}

//--------------merged mapper----------------------------
public static class AnonymizeDistinctMergedMapper extends
 Mapper<Object, Text, TaggedText, Text> {
 private static final Text DISTINCT_OUT_VALUE = new Text();
 private Random rndm = new Random();
 private TaggedText anonymizeOutkey = new TaggedText(),
 distinctOutkey = new TaggedText();
 private Text anonymizeOutvalue = new Text();
 public void map(Object key, Text value, Context context)
 throws IOException, InterruptedException {
 anonymizeMap(key, value, context);
 distinctMap(key, value, context);
 }
 private void anonymizeMap(Object key, Text value, Context context)
 throws IOException, InterruptedException {
 Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
 .toString());
 if (parsed.size() > 0) {
 StringBuilder bldr = new StringBuilder();
 bldr.append("<row ");
 for (Entry<String, String> entry : parsed.entrySet()) {
 if (entry.getKey().equals("UserId")
 || entry.getKey().equals("Id")) {
 // ignore these fields
 } else if (entry.getKey().equals("CreationDate")) {
 // Strip out the time, anything after the 'T'
 // in the value
 bldr.append(entry.getKey()
 + "=\""
+ entry.getValue().substring(0,
 entry.getValue().indexOf('T'))
 + "\" ");
 } else {
 // Otherwise, output this.
bldr.append(entry.getKey() + "=\"" + entry.
 getValue() + "\" ");
 }
 }
 bldr.append(">");
 anonymizeOutkey.setTag("A");
 anonymizeOutkey.setText(Integer.toString(rndm.nextInt()));
 anonymizeOutvalue.set(bldr.toString());
 context.write(anonymizeOutkey, anonymizeOutvalue);
 }
 }

private void distinctMap(Object key, Text value, Context context)
 throws IOException, InterruptedException {
 Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
 .toString());
 // Otherwise, set our output key to the user's id,
 // tagged with a "D"
 distinctOutkey.setTag("D");
 distinctOutkey.setText(parsed.get("UserId"));
 // Write the user's id with a null value
 context.write(distinctOutkey, DISTINCT_OUT_VALUE);
 }
}

public static class AnonymizeDistinctMergedReducer extends
 Reducer<TaggedText, Text, Text, NullWritable> {
 private MultipleOutputs<Text, NullWritable> mos = null;
 protected void setup(Context context) throws IOException,
 InterruptedException {
 mos = new MultipleOutputs<Text, NullWritable>(context);
 }
 protected void reduce(TaggedText key, Iterable<Text> values,
 Context context) throws IOException, InterruptedException {

 if (key.getTag().equals("A")) {
 anonymizeReduce(key.getText(), values, context);
 } else {
 distinctReduce(key.getText(), values, context);
 }
 }
 private void anonymizeReduce(Text key, Iterable<Text> values,
 Context context) throws IOException, InterruptedException {
 for (Text value : values) {
 mos.write(MULTIPLE_OUTPUTS_ANONYMIZE, value,
 NullWritable.get(), MULTIPLE_OUTPUTS_ANONYMIZE + "/part");
 }
 }
 private void distinctReduce(Text key, Iterable<Text> values,
 Context context) throws IOException, InterruptedException {
 mos.write(MULTIPLE_OUTPUTS_DISTINCT, key, NullWritable.get(),
 MULTIPLE_OUTPUTS_DISTINCT + "/part");
 }
 protected void cleanup(Context context) throws IOException,
 InterruptedException {
 mos.close();
 }
}

//---------------druver------------------------------

public static void main(String[] args) throws Exception {
 // Configure the merged job
 Job job = new Job(new Configuration(), "MergedJob");
 job.setJarByClass(MergedJobDriver.class);
 job.setMapperClass(AnonymizeDistinctMergedMapper.class);
 job.setReducerClass(AnonymizeDistinctMergedReducer.class);
 job.setNumReduceTasks(10);
 TextInputFormat.setInputPaths(job, new Path(args[0]));
 TextOutputFormat.setOutputPath(job, new Path(args[1]));
 MultipleOutputs.addNamedOutput(job, MULTIPLE_OUTPUTS_ANONYMIZE,
 TextOutputFormat.class, Text.class, NullWritable.class);
 MultipleOutputs.addNamedOutput(job, MULTIPLE_OUTPUTS_DISTINCT,
 TextOutputFormat.class, Text.class, NullWritable.class);
 job.setOutputKeyClass(TaggedText.class);

 job.setOutputValueClass(Text.class);
 System.exit(job.waitForCompletion(true) ? 0 : 1);
}


