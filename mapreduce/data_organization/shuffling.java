public static class AnonymizeMapper extends
 Mapper<Object, Text, IntWritable, Text> {
 private IntWritable outkey = new IntWritable();
 private Random rndm = new Random();
 private Text outvalue = new Text();
 public void map(Object key, Text value, Context context)
 throws IOException, InterruptedException {
 Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
 .toString());
 if (parsed.size() > 0) {
 StringBuilder bldr = new StringBuilder();
 // Create the start of the record
 bldr.append("<row ");
 // For each XML attribute
 for (Entry<String, String> entry : parsed.entrySet()) {
 // If it is a user ID or row ID, ignore it
 if (entry.getKey().equals("UserId")
 || entry.getKey().equals("Id")) {
 } else if (entry.getKey().equals("CreationDate")) {
 // If it is a CreationDate, remove the time from the date
 // i.e., anything after the 'T' in the value
 bldr.append(entry.getKey()
 + "=\""

 + entry.getValue().substring(0,
 entry.getValue().indexOf('T')) + "\" ");
 } else {
 // Otherwise, output the attribute and value as is
 bldr.append(entry.getKey() + "=\"" + entry.getValue()
 + "\" ");
 }
 }
 // Add the /> to finish the record
 bldr.append("/>");
 // Set the sort key to a random value and output
 outkey.set(rndm.nextInt());
 outvalue.set(bldr.toString());
 context.write(outkey, outvalue);
 }
 }
}

//------------------reducer-----------------------------------

public static class ValueReducer extends
 Reducer<IntWritable, Text, Text, NullWritable> {
 protected void reduce(IntWritable key, Iterable<Text> values,
 Context context) throws IOException, InterruptedException {
 for (Text t : values) {
 context.write(t, NullWritable.get());
 }
 }
}


