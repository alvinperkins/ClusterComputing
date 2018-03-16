SELECT MIN(numericalcol1), MAX(numericalcol1),
 COUNT(*) FROM table GROUP BY groupcol2;

public class MinMaxCountTuple implements Writable {
 private Date min = new Date();
 private Date max = new Date();
 private long count = 0;
 private final static SimpleDateFormat frmt = new SimpleDateFormat(
 "yyyy-MM-dd'T'HH:mm:ss.SSS");
 public Date getMin() {
 return min;
 }
 public void setMin(Date min) {
 this.min = min;
 }
 public Date getMax() {
 return max;
 }
 public void setMax(Date max) {
 this.max = max;
 }
 public long getCount() {
 return count;
 }
 public void setCount(long count) {
 this.count = count;
 }
 public void readFields(DataInput in) throws IOException {
 // Read the data out in the order it is written,
 // creating new Date objects from the UNIX timestamp
 min = new Date(in.readLong());
 max = new Date(in.readLong());
 count = in.readLong();
 }
 public void write(DataOutput out) throws IOException {
 // Write the data out in the order it is read,
 // using the UNIX timestamp to represent the Date
 out.writeLong(min.getTime());
 out.writeLong(max.getTime());
 out.writeLong(count);
 }
 public String toString() {
 return frmt.format(min) + "\t" + frmt.format(max) + "\t" + count;
 }
}








//----------------------mapper code-----------------------------------





 public static class MinMaxCountMapper extends
 Mapper<Object, Text, Text, MinMaxCountTuple> {
 // Our output key and value Writables
 private Text outUserId = new Text();
 private MinMaxCountTuple outTuple = new MinMaxCountTuple();
 // This object will format the creation date string into a Date object
 private final static SimpleDateFormat frmt =
 new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
 public void map(Object key, Text value, Context context)
 throws IOException, InterruptedException {
 Map<String, String> parsed = transformXmlToMap(value.toString());
 // Grab the "CreationDate" field since it is what we are finding
 // the min and max value of
 String strDate = parsed.get("CreationDate");
 // Grab the “UserID” since it is what we are grouping by
 String userId = parsed.get("UserId");
 // Parse the string into a Date object
 Date creationDate = frmt.parse(strDate);
 // Set the minimum and maximum date values to the creationDate
 outTuple.setMin(creationDate);
 outTuple.setMax(creationDate);
 // Set the comment count to 1
 outTuple.setCount(1);
 // Set our user ID as the output key
 outUserId.set(userId);
 // Write out the hour and the average comment length
 context.write(outUserId, outTuple);
 }
}



//---------------reducer code----------------

public static class MinMaxCountReducer extends
 Reducer<Text, MinMaxCountTuple, Text, MinMaxCountTuple> {
 // Our output value Writable
 private MinMaxCountTuple result = new MinMaxCountTuple();
 public void reduce(Text key, Iterable<MinMaxCountTuple> values,
 Context context) throws IOException, InterruptedException {
 // Initialize our result
 result.setMin(null);
 result.setMax(null);
 result.setCount(0);
 int sum = 0;
 // Iterate through all input values for this key
 for (MinMaxCountTuple val : values) {
 // If the value's min is less than the result's min
 // Set the result's min to value's
 if (result.getMin() == null |
val.getMin().compareTo(result.getMin()) < 0) {
 result.setMin(val.getMin());
 }
 // If the value's max is more than the result's max
 // Set the result's max to value's
 if (result.getMax() == null ||
 val.getMax().compareTo(result.getMax()) > 0) {
 result.setMax(val.getMax());
 }
 // Add to our sum the count for value
 sum += val.getCount();
 }
 // Set our count to the number of input values
 result.setCount(sum);
 context.write(key, result);
 }
}



//------------------average mapper----------------

public static class AverageMapper extends
 Mapper<Object, Text, IntWritable, CountAverageTuple> {
 private IntWritable outHour = new IntWritable();
 private CountAverageTuple outCountAverage = new CountAverageTuple();
 private final static SimpleDateFormat frmt = new SimpleDateFormat(
 "yyyy-MM-dd'T'HH:mm:ss.SSS");
 public void map(Object key, Text value, Context context)
 throws IOException, InterruptedException {
 Map<String, String> parsed = transformXmlToMap(value.toString());
 // Grab the "CreationDate" field,
 // since it is what we are grouping by
 String strDate = parsed.get("CreationDate");
 // Grab the comment to find the length
 String text = parsed.get("Text");

 // get the hour this comment was posted in
 Date creationDate = frmt.parse(strDate);
 outHour.set(creationDate.getHours());
 // get the comment length
 outCountAverage.setCount(1);
 outCountAverage.setAverage(text.length());
 // write out the hour with the comment length
 context.write(outHour, outCountAverage);
 }
}

//----------------------average reducer-------------------------

public static class AverageReducer extends
 Reducer<IntWritable, CountAverageTuple,
 IntWritable, CountAverageTuple> {

 private CountAverageTuple result = new CountAverageTuple();
 public void reduce(IntWritable key, Iterable<CountAverageTuple> values,
 Context context) throws IOException, InterruptedException {
 float sum = 0;
 float count = 0;
 // Iterate through all input values for this key
 for (CountAverageTuple val : values) {
 sum += val.getCount() * val.getAverage();
 count += val.getCount();
 }
 result.setCount(count);
 result.setAverage(sum / count);
 context.write(key, result);
 }
}


//--------------------medianStdDev---------------------------
