// Use MultipleInputs to set which input uses what mapper
// This will keep parsing of each data set separate from a logical standpoint
// The first two elements of the args array are the two inputs
MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class,
 UserJoinMapper.class);
MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class,
 CommentJoinMapper.class);
job.getConfiguration()..set("join.type", args[2]);

public static class UserJoinMapper extends Mapper<Object, Text, Text, Text> {
 private Text outkey = new Text();
 private Text outvalue = new Text();
 public void map(Object key, Text value, Context context)
 throws IOException, InterruptedException {
 // Parse the input string into a nice map
 Map<String, String> parsed =
 MRDPUtils.transformXmlToMap(value.toString());
 String userId = parsed.get("Id");

 // The foreign join key is the user ID
 outkey.set(userId);
 // Flag this record for the reducer and then output
 outvalue.set("A" + value.toString());
 context.write(outkey, outvalue);
 }
}

public static class CommentJoinMapper extends
 Mapperlt;Object, Text, Text, Text> {
 private Text outkey = new Text();
 private Text outvalue = new Text();
 public void map(Object key, Text value, Context context)
 throws IOException, InterruptedException {
 Map<String, String> parsed = transformXmlToMap(value.toString());
 // The foreign join key is the user ID
 outkey.set( parsed.get("UserId"));
 // Flag this record for the reducer and then output
 outvalue.set("B" + value.toString());
 context.write(outkey, outvalue);
 }
}

//---------------reducer-----------------------------

public static class UserJoinReducer extends Reducer<Text, Text, Text, Text> {
 private static final Text EMPTY_TEXT = Text("");
 private Text tmp = new Text();
 private ArrayList<Text> listA = new ArrayList<Text>();
 private ArrayList<Text> listB = new ArrayList<Text>();

private String joinType = null;
 public void setup(Context context) {
 // Get the type of join from our configuration
 joinType = context.getConfiguration().get("join.type");
 }
 public void reduce(Text key, Iterable<Text> values, Context context)
 throws IOException, InterruptedException {
 // Clear our lists
 listA.clear();
 listB.clear();
 // iterate through all our values, binning each record based on what
 // it was tagged with. Make sure to remove the tag!
 while (values.hasNext()) {
 tmp = values.next();
 if (tmp.charAt(0) == 'A') {
 listA.add(new Text(tmp.toString().substring(1)));
 } else if (tmp.charAt('0') == 'B') {
 listB.add(new Text(tmp.toString().substring(1)));
 }
 }
 // Execute our join logic now that the lists are filled
 executeJoinLogic(context);
 }
 private void executeJoinLogic(Context context)
 throws IOException, InterruptedException {
 ...
 }

if (joinType.equalsIgnoreCase("inner")) {
 // If both lists are not empty, join A with B
 if (!listA.isEmpty() && !listB.isEmpty()) {
 for (Text A : listA) {
 for (Text B : listB) {
 context.write(A, B);

 }
 }
 }
} ...

//--------------left outer--------------------------------------
... else if (joinType.equalsIgnoreCase("leftouter")) {
 // For each entry in A,
 for (Text A : listA) {
 // If list B is not empty, join A and B
 if (!listB.isEmpty()) {
 for (Text B : listB) {
 context.write(A, B);
 }
 } else {
 // Else, output A by itself
 context.write(A, EMPTY_TEXT);
 }
 }
} ...
//-------------right outer join----------------------

... else if (joinType.equalsIgnoreCase("rightouter")) {
 // For each entry in B,
 for (Text B : listB) {
 // If list A is not empty, join A and B
 if (!listA.isEmpty()) {
 for (Text A : listA) {
 context.write(A, B);
 }
 } else {
 // Else, output B by itself
 context.write(EMPTY_TEXT, B);
 }
 }
} ...
//-----------------full join---------------------------

else if (joinType.equalsIgnoreCase("fullouter")) {
 // If list A is not empty
 if (!listA.isEmpty()) {
 // For each entry in A
 for (Text A : listA) {
 // If list B is not empty, join A with B
 if (!listB.isEmpty()) {

for (Text B : listB) {
 context.write(A, B);
 }
 } else {
 // Else, output A by itself
 context.write(A, EMPTY_TEXT);
 }
 }
 } else {
 // If list A is empty, just output B
 for (Text B : listB) {
 context.write(EMPTY_TEXT, B);
 }
 }
} ...

//--------------anti join-----------------------


... else if (joinType.equalsIgnoreCase("anti")) {
 // If list A is empty and B is empty or vice versa
 if (listA.isEmpty() ^ listB.isEmpty()) {
 // Iterate both A and B with null values
 // The previous XOR check will make sure exactly one of
 // these lists is empty and therefore the list will be skipped
 for (Text A : listA) {
 context.write(A, EMPTY_TEXT);
 }
 for (Text B : listB) {
 context.write(EMPTY_TEXT, B);
 }
 }
} ...



