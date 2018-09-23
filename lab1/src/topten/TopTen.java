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

    public static class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {
		// Stores a map of user reputation to the record
		TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Map<String, String> row = transformXmlToMap(value.toString());
			String id = row.get("Id");
			if (id == null || id == "") {
				return;
			}

			String rep = row.get("Reputation");
			repToRecordMap.put(Integer.parseInt(rep), value);
			if (repToRecordMap.size() > 10) {
				repToRecordMap.remove(repToRecordMap.firstKey());
			}
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			// Output our ten records to the reducers with a null key
			//int count = 0;
			for (Entry<Integer, Text> entry : repToRecordMap.descendingMap().entrySet()) {
				context.write(NullWritable.get(), entry.getValue());
				//if (++count == 10) {
				//	break;
				//}
			}
			repToRecordMap.clear();
		}
    }

    public static class TopTenReducer extends TableReducer<NullWritable, Text, NullWritable> {
		// Stores a map of user reputation to the record
		private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

		public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text val : values) {
				Map<String, String> row = transformXmlToMap(val.toString());
				String rep = row.get("Reputation");
				repToRecordMap.put(Integer.parseInt(rep), val);
				if (repToRecordMap.size() > 10) {
					repToRecordMap.remove(repToRecordMap.firstKey());
				}
			}
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {

			Integer count = 1;
			for (Entry<Integer, Text> entry : repToRecordMap.descendingMap().entrySet()) {
				Map<String, String> row = transformXmlToMap(entry.getValue().toString());
				String rep = row.get("Reputation");
				String id = row.get("Id");

				Put insHBase = new Put(new Text((count++).toString()).getBytes());
				insHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(Integer.parseInt(id)));
				insHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("rep"), Bytes.toBytes(Integer.parseInt(rep)));
				context.write(NullWritable.get(), insHBase);
			}
		}
    }

    public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "top10");
		job.setNumReduceTasks(1);
		job.setJarByClass(TopTen.class);
		job.setMapperClass(TopTenMapper.class);
		job.setCombinerClass(TopTenReducer.class);
		job.setReducerClass(TopTenReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		// define scan and define column families to scan
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("info"));

		FileInputFormat.addInputPath(job, new Path(args[0]));
		//FileOutputFormat.setOutputPath(job, new Path(args[1]));

		TableMapReduceUtil.initTableReducerJob("topten", TopTenReducer.class, job);
		job.waitForCompletion(true);

    	/*

		// WordCount
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);



    	 // HBase
		 Configuration conf = HBaseConfiguration.create();
		 // define scan and define column families to scan
		 Scan scan = new Scan();
		 scan.addFamily(Bytes.toBytes("cf"));
		 Job job = Job.getInstance(conf);
		 job.setJarByClass(HBaseMapReduce.class);
		 // define input hbase table
		 TableMapReduceUtil.initTableMapperJob("test1", scan, hbaseMapper.class, Text.class, IntWritable.class, job);
		 // define output table
		 TableMapReduceUtil.initTableReducerJob("test2", hbaseReducer.class, job);
		 job.waitForCompletion(true);
    	*
    	* */

		//job.setNumReduceTasks(1)
    }
}
