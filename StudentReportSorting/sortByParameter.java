import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class sortByParameter 
{
	
	public static class paraMapper extends Mapper<LongWritable, Text, NullWritable, Text> 
	{
		private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			String number = context.getConfiguration().get("parameter");
			int n = Integer.parseInt(number);
			String record = value.toString();
			String[] parts = record.split(",");
			int myKey = Integer.parseInt(parts[2]);
			repToRecordMap.put(myKey, new Text(value));
			if (repToRecordMap.size() > n) 
			{
				repToRecordMap.remove(repToRecordMap.firstKey());
			}
		}
		protected void cleanup(Context context) throws IOException,InterruptedException 
		{
		// Output our 'n' records to the reducers with a null key
		for (Text t : repToRecordMap.values()) {
		context.write(NullWritable.get(), t);
			}
		}
	}

	public static class paraReducer extends	Reducer<NullWritable, Text, NullWritable, Text> 
	{
		private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

		public void reduce(NullWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException 
		{
			String number = context.getConfiguration().get("parameter");
			int n = Integer.parseInt(number);
				for (Text value : values) 
				{
					String record = value.toString();
					String[] parts = record.split(",");
					int myKey = Integer.parseInt(parts[2]);
					repToRecordMap.put(myKey, new Text(value));
				if (repToRecordMap.size() > n) 
				{
							repToRecordMap.remove(repToRecordMap.firstKey());
				}
				}
				for (Text t : repToRecordMap.descendingMap().values()) 
				{
					// Output our five records to the file system with a null key
					context.write(NullWritable.get(), t);
				}
		}
	}
	
	public static void main(String[] args) throws Exception 
	{
		
		Configuration conf = new Configuration();
		if(args.length > 2)
	      {
			  conf.set("parameter", args[2]);
			  String number = conf.get("parameter");
			  if(Integer.parseInt(number)>5)
				{
				  System.out.println("Enter value less than 5");
				  System.exit(0);
				}
	      }
		
		Job job = Job.getInstance(conf, "Top by Parameter Records");
	    job.setJarByClass(sortByParameter.class);
	    job.setMapperClass(paraMapper.class);
	    job.setReducerClass(paraReducer.class);
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}