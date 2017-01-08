import java.io.IOException;


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


public class distinct {
	public static class Top5Mapper extends
	Mapper<LongWritable, Text, Text, NullWritable> {

		public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
			String record = value.toString();
			String[] parts = record.split(",");
			String city = parts[3];
			context.write(new Text(city), NullWritable.get());	
		}
	}

	public static class Top5Reducer extends
	Reducer<Text, NullWritable, Text, NullWritable> {
		
		public void reduce(Text key, Iterable<NullWritable> values,
				Context context) throws IOException, InterruptedException {
				context.write(key, NullWritable.get());
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Top 5 Records");
	    job.setJarByClass(distinct.class);
	    job.setMapperClass(Top5Mapper.class);
	    job.setReducerClass(Top5Reducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(NullWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}