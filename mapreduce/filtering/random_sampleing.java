
public static class SRSMapper
 extends Mapper<Object, Text, NullWritable, Text> {
private Random rands = new Random();
 private Double percentage;
 protected void setup(Context context) throws IOException,
 InterruptedException {
 // Retrieve the percentage that is passed in via the configuration
 // like this: conf.set("filter_percentage", .5);
 // for .5%
 String strPercentage = context.getConfiguration()
 .get("filter_percentage");
 percentage = Double.parseDouble(strPercentage) / 100.0;
 }
 public void map(Object key, Text value, Context context)
 throws IOException, InterruptedException {
 if (rands.nextDouble() < percentage) {
 context.write(NullWritable.get(), value);
 }
 }
}



