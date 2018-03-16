// Configure the MultipleOutputs by adding an output called "bins"
// With the proper output format and mapper key/value pairs
MultipleOutputs.addNamedOutput(job, "bins", TextOutputFormat.class,
 Text.class, NullWritable.class);
// Enable the counters for the job
// If there are a significant number of different named outputs, this
// should be disabled
MultipleOutputs.setCountersEnabled(job, true);
// Map-only job
job.setNumReduceTasks(0);
...


//----------------------Mapper------------------------

public static class BinningMapper extends
 Mapper<Object, Text, Text, NullWritable> {
 private MultipleOutputs<Text, NullWritable> mos = null;
 protected void setup(Context context) {
 // Create a new MultipleOutputs using the context object
 mos = new MultipleOutputs(context);
 }
 protected void map(Object key, Text value, Context context)
 throws IOException, InterruptedException {
 Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
 .toString());
 String rawtags = parsed.get("Tags");
 // Tags are delimited by ><. i.e. <tag1><tag2><tag3>
 String[] tagTokens = StringEscapeUtils.unescapeHtml(rawtags).split(
 "><");
 // For each tag
 for (String tag : tagTokens) {
 // Remove any > or < from the token
 String groomed = tag.replaceAll(">|<", "").toLowerCase();
 // If this tag is one of the following, writte to the named bin
 if (groomed.equalsIgnoreCase("hadoop")) {
mos.write("bins", value, NullWritable.get(), "hadoop-tag");
 }
 if (groomed.equalsIgnoreCase("pig")) {
 mos.write("bins", value, NullWritable.get(), "pig-tag");
 }
 if (groomed.equalsIgnoreCase("hive")) {
 mos.write("bins", value, NullWritable.get(), "hive-tag");
 }
 if (groomed.equalsIgnoreCase("hbase")) {
 mos.write("bins", value, NullWritable.get(), "hbase-tag");
 }
 }
 // Get the body of the post
 String post = parsed.get("Body");
 // If the post contains the word "hadoop", write it to its own bin
 if (post.toLowerCase().contains("hadoop")) {
 mos.write("bins", value, NullWritable.get(), "hadoop-post");
 }
 }
 protected void cleanup(Context context) throws IOException,
 InterruptedException {
 // Close multiple outputs!
 mos.close();
 }
}


