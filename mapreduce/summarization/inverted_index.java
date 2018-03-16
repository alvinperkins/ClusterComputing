 Mapper<Object, Text, Text, Text> {
 private Text link = new Text();
 private Text outkey = new Text();
 public void map(Object key, Text value, Context context)
 throws IOException, InterruptedException {
 Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
 .toString());
 // Grab the necessary XML attributes
 String txt = parsed.get("Body");
 String posttype = parsed.get("PostTypeId");
 String row_id = parsed.get("Id");

// if the body is null, or the post is a question (1), skip
 if (txt == null || (posttype != null && posttype.equals("1"))) {
 return;
 }
 // Unescape the HTML because the SO data is escaped.
 txt = StringEscapeUtils.unescapeHtml(txt.toLowerCase());

 link.set(getWikipediaURL(txt));
 outkey.set(row_id);
 context.write(link, outkey);
 }
}

public static class Concatenator extends Reducer<Text,Text,Text,Text> {
 private Text result = new Text();
 public void reduce(Text key, Iterable<Text> values, Context context)
 throws IOException, InterruptedException {
 StringBuilder sb = new StringBuilder();
 boolean first = true;
 for (Text id : values) {
 if (first) {
 first = false;
 } else {
 sb.append(" ");
 }
 sb.append(id.toString());
 }
 result.set(sb.toString());
 context.write(key, result);
 }
}


