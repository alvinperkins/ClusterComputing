

//--------------Mapper-------------------------

public static class DistinctUserMapper extends
 Mapper<Object, Text, Text, NullWritable> {
 private Text outUserId = new Text();
 public void map(Object key, Text value, Context context)
 throws IOException, InterruptedException {

 Map<String, String> parsed = transformXmlToMap(value.toString());
 // Get the value for the UserId attribute
 String userId = parsed.get("UserId");
 // Set our output key to the user's id
 outUserId.set(userId);
 // Write the user's id with a null value
 context.write(outUserId, NullWritable.get());
 }
}

//------------------reducer---------------------

public static class DistinctUserReducer extends
 Reducer<Text, NullWritable, Text, NullWritable> {
 public void reduce(Text key, Iterable<NullWritable> values,
 Context context) throws IOException, InterruptedException {
 // Write the user's id with a null value
 context.write(key, NullWritable.get());
 }
}


