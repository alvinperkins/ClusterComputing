public static void main(String[] args) throws Exception {
 Configuration conf = new Configuration();
 Job job = new Job(conf, "PostCommentHierarchy");
 job.setJarByClass(PostCommentBuildingDriver.class);
 MultipleInputs.addInputPath(job, new Path(args[0]),
 TextInputFormat.class, PostMapper.class);
 MultipleInputs.addInputPath(job, new Path(args[1]),
 TextInputFormat.class, CommentMapper.class);
 job.setReducerClass(UserJoinReducer.class);
 job.setOutputFormatClass(TextOutputFormat.class);
 TextOutputFormat.setOutputPath(job, new Path(args[2]));
 job.setOutputKeyClass(Text.class);

 job.setOutputValueClass(Text.class);
 System.exit(job.waitForCompletion(true) ? 0 : 2);
}

//--------------postMapper---------------------------------------

public static class PostMapper extends Mapper<Object, Text, Text, Text> {
 private Text outkey = new Text();
 private Text outvalue = new Text();
 public void map(Object key, Text value, Context context)
 throws IOException, InterruptedException {
 Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
 .toString());
 // The foreign join key is the post ID
 outkey.set(parsed.get("Id"));
 // Flag this record for the reducer and then output
 outvalue.set("P" + value.toString());
 context.write(outkey, outvalue);
 }
}
public static class CommentMapper extends Mapper<Object, Text, Text, Text> {
 private Text outkey = new Text();
 private Text outvalue = new Text();
 public void map(Object key, Text value, Context context)
 throws IOException, InterruptedException {
 Map<String, String> parsed = MRDPUtils.transformXmlToMap(value
 .toString());
 // The foreign join key is the post ID
 outkey.set(parsed.get("PostId"));
 // Flag this record for the reducer and then output
 outvalue.set("C" + value.toString());
 context.write(outkey, outvalue);
 }
}

//----------------------reducer--------------------------------

public static class PostCommentHierarchyReducer extends
 Reducer<Text, Text, Text, NullWritable> {
 private ArrayList<String> comments = new ArrayList<String>();
 private DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
 private String post = null;
 public void reduce(Text key, Iterable<Text> values, Context context)
 throws IOException, InterruptedException {
 // Reset variables
 post = null;
 comments.clear();
 // For each input value
 for (Text t : values) {
 // If this is the post record, store it, minus the flag
 if (t.charAt(0) == 'P') {
 post = t.toString().substring(1, t.toString().length())
 .trim();
 } else {
 // Else, it is a comment record. Add it to the list, minus
 // the flag
 comments.add(t.toString()
 .substring(1, t.toString().length()).trim());
 }
 }
 // If there are no comments, the comments list will simply be empty.
 // If post is not null, combine post with its comments.
 if (post != null) {
 // nest the comments underneath the post element
 String postWithCommentChildren = nestElements(post, comments);
 // write out the XML
 context.write(new Text(postWithCommentChildren),
 NullWritable.get());
 }
 }
 


//----------------------------------------------------
private String nestElements(String post, List<String> comments) {
 // Create the new document to build the XML
 DocumentBuilder bldr = dbf.newDocumentBuilder();
 Document doc = bldr.newDocument();
 // Copy parent node to document
 Element postEl = getXmlElementFromString(post);
 Element toAddPostEl = doc.createElement("post");
 // Copy the attributes of the original post element to the new one
 copyAttributesToElement(postEl.getAttributes(), toAddPostEl);
 // For each comment, copy it to the "post" node
 for (String commentXml : comments) {
 Element commentEl = getXmlElementFromString(commentXml);
 Element toAddCommentEl = doc.createElement("comments");
 // Copy the attributes of the original comment element to
 // the new one
 copyAttributesToElement(commentEl.getAttributes(),
 toAddCommentEl);
 // Add the copied comment to the post element
 toAddPostEl.appendChild(toAddCommentEl);
 }
 // Add the post element to the document
 doc.appendChild(toAddPostEl);
 // Transform the document into a String of XML and return
 return transformDocumentToString(doc);
 }
 private Element getXmlElementFromString(String xml) {
 // Create a new document builder
 DocumentBuilder bldr = dbf.newDocumentBuilder();
 return bldr.parse(new InputSource(new StringReader(xml)))
 .getDocumentElement();
 }
 private void copyAttributesToElement(NamedNodeMap attributes,
 Element element) {
 // For each attribute, copy it to the element
 for (int i = 0; i < attributes.getLength(); ++i) {

 Attr toCopy = (Attr) attributes.item(i);
 element.setAttribute(toCopy.getName(), toCopy.getValue());
 }
 }
 private String transformDocumentToString(Document doc) {
 TransformerFactory tf = TransformerFactory.newInstance();
 Transformer transformer = tf.newTransformer();
 transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION,
 "yes");
 StringWriter writer = new StringWriter();
 transformer.transform(new DOMSource(doc), new StreamResult(
 writer));
 // Replace all new line characters with an empty string to have
 // one record per line.
 return writer.getBuffer().toString().replaceAll("\n|\r", "");
 }
}


