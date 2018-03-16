public static class GrepMapper
 extends Mapper<Object, Text, NullWritable, Text> {
 private String mapRegex = null;
 public void setup(Context context) throws IOException,
 InterruptedException {

 mapRegex = context.getConfiguration().get("mapregex");
 }

 public void map(Object key, Text value, Context context)
 throws IOException, InterruptedException {
 if (value.toString().matches(mapRegex)) {
 context.write(NullWritable.get(), value);
 }
 }
}


