package topten;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.HashMap;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

public class TopTen {

    // This helper function parses the stackoverflow into a Map for us.
    public static Map<String, String> transformXmlToMap(String xml) {
		Map<String, String> map = new HashMap<String, String>();
		try {
			String[] tokens = xml.trim().substring(5, xml.trim().length() - 3).split("\"");
			for (int i = 0; i < tokens.length - 1; i += 2) {
				String key = tokens[i].trim();
				String val = tokens[i + 1];
				map.put(key.substring(0, key.length() - 1), val);
			}
		} catch (StringIndexOutOfBoundsException e) {
			System.err.println(xml);
		}

		return map;
    }

    // Mapper class
    public static class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {
		// Stores a map of user reputation to the record
		TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

		// Map function deals with rows of the xml file, in a parallel manner.
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			// Input XML to string, then map
			Map<String, String> row = transformXmlToMap(value.toString());

			// Extract variables of interest
			String id = row.get("Id");
			String rep = row.get("Reputation");

			// Stop if id is null or an empty string
			if (id == null || id == "") {
				return;
			}

			// Put reputation and id pair into treemap
			repToRecordMap.put(Integer.parseInt(rep), new Text(value));

			// Remove first key if treemap gets bigger than 10 records
			if (repToRecordMap.size() > 10){
				repToRecordMap.remove(repToRecordMap.firstKey());
			}
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			// Output our ten records to the reducers with a null key
			for (Entry<Integer, Text> entry : repToRecordMap.descendingMap().entrySet()) {
				context.write(NullWritable.get(), entry.getValue());
			}
		}
    }

    // Reducer class
    public static class TopTenReducer extends TableReducer<NullWritable, Text, NullWritable> {
		// Stores a map of user reputation to the record
		private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

		public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			// For each value in iterator
			for (Text val : values) {

				// Get row from xml
				Map<String, String> row = transformXmlToMap(val.toString());

				// Get reputation parameter
				String rep = row.get("Reputation");

				// Insert pair into treemap
				repToRecordMap.put(Integer.parseInt(rep), new Text(val));

				// Keep only 10 highest reputation records in tree
				if (repToRecordMap.size() > 10) {
					repToRecordMap.remove(repToRecordMap.firstKey());
				}
			}
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {

			// Start row count at 0
			Integer count = 0;

			// For every entry in the treemap, in a descending order
			for (Entry<Integer, Text> entry : repToRecordMap.descendingMap().entrySet()) {
				
				// Get data
				Map<String, String> row = transformXmlToMap(entry.getValue().toString());
				String rep = row.get("Reputation");
				String id = row.get("Id");

				// Put into HBase table
				Put insHBase = new Put(new Text((count++).toString()).getBytes());
				insHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes((id)));
				insHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("rep"), Bytes.toBytes((rep)));
				context.write(NullWritable.get(), insHBase);
			}
		}
    }

    public static void main(String[] args) throws Exception {

    	// Set configuration
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "top10");

		// Set reducer parallel tasks to one, to avoid duplicates in final table
		job.setNumReduceTasks(1);

		// Set all classes
		job.setJarByClass(TopTen.class);
		job.setMapperClass(TopTenMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(TopTenReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		// Define input file
		FileInputFormat.addInputPath(job, new Path(args[0]));

		// Define output HBase table
		TableMapReduceUtil.initTableReducerJob("topten", TopTenReducer.class, job);

		// Wait until job is complete
		job.waitForCompletion(true);
    }
}