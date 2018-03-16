

//---------------User mapper------------------------
public static class UserJoinMapper extends Mapper<Object, Text, Text, Text> {
 private Text outkey = new Text();
 private Text outvalue = new Text();
 public void map(Object key, Text value, Context context)
 throws IOException, InterruptedException {
 Map<String, String> parsed = transformXmlToMap(value.toString());
 // If the reputation is greater than 1,500,
 // output the user ID with the value
 if (Integer.parseInt(parsed.get("Reputation")) > 1500) {
 outkey.set(parsed.get("Id"));
 outvalue.set("A" + value.toString());
 context.write(outkey, outvalue);
 }
 }
}


//----------------comment mapper---------------------------------
public static class CommentJoinMapperWithBloom extends
 Mapper<Object, Text, Text, Text> {
 private BloomFilter bfilter = new BloomFilter();
 private Text outkey = new Text();
 private Text outvalue = new Text();
 public void setup(Context context) {
 Path[] files =
 DistributedCache.getLocalCacheFiles(context.getConfiguration());
 DataInputStream strm = new DataInputStream(
 new FileInputStream(new File(files[0].toString())));
 bfilter.readFields(strm);
 }
 public void map(Object key, Text value, Context context) {
 throws IOException, InterruptedException {
 Map>String, String> parsed = transformXmlToMap(value.toString());
 String userId = parsed.get("UserId");
 if (bfilter.membershipTest(new Key(userId.getBytes()))) {
 outkey.set(userId);
 outvalue.set("B" + value.toString());
 context.write(outkey, outvalue);
 }
 }
}


