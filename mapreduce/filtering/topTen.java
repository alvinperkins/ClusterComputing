 Mapper<Object, Text, NullWritable, Text> {
 // Stores a map of user reputation to the record
 private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();
 public void map(Object key, Text value, Context context)
 throws IOException, InterruptedException {
 Map<String, String> parsed = transformXmlToMap(value.toString());
 String userId = parsed.get("Id");
 String reputation = parsed.get("Reputation");
 // Add this record to our map with the reputation as the key
 repToRecordMap.put(Integer.parseInt(reputation), new Text(value));
 // If we have more than ten records, remove the one with the lowest rep
 // As this tree map is sorted in descending order, the user with
 // the lowest reputation is the last key.
 if (repToRecordMap.size() > 10) {

 repToRecordMap.remove(repToRecordMap.firstKey());
 }
 }
 protected void cleanup(Context context) throws IOException,
 InterruptedException {
 // Output our ten records to the reducers with a null key
 for (Text t : repToRecordMap.values()) {
 context.write(NullWritable.get(), t);
 }
 }
}

//------------------reducer--------------------------


 Reducer<NullWritable, Text, NullWritable, Text> {
 // Stores a map of user reputation to the record
 // Overloads the comparator to order the reputations in descending order
 private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();
 public void reduce(NullWritable key, Iterable<Text> values,
 Context context) throws IOException, InterruptedException {
 for (Text value : values) {
 Map<String, String> parsed = transformXmlToMap(value.toString());
 repToRecordMap.put(Integer.parseInt(parsed.get("Reputation")),
 new Text(value));
 // If we have more than ten records, remove the one with the lowest rep
 // As this tree map is sorted in descending order, the user with
 // the lowest reputation is the last key.
 if (repToRecordMap.size() > 10) {
 repToRecordMap.remove(repToRecordMap.firstKey());
 }
 }
 for (Text t : repToRecordMap.descendingMap().values()) {
 // Output our ten records to the file system with a null key
 context.write(NullWritable.get(), t);
 }
 }
}




