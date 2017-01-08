

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class adDataAnalysis 
{

	public static class facebookMapper extends Mapper<Object, Text, Text, Text> 
	{
		public void map(Object key, Text value, Context context)throws IOException, InterruptedException 
		{
			String record = value.toString();
			String[] parts = record.split(",");
			context.write(new Text(parts[0]), new Text("facebook\t"+parts[1]));
		}
	}

	public static class twitterMapper extends Mapper<Object, Text, Text, Text> 
	{
		public void map(Object key, Text value, Context context)throws IOException, InterruptedException 
		{
			String record = value.toString();
			String[] parts = record.split(",");
			context.write(new Text(parts[0]), new Text("twitter\t"+parts[1]));
		}
	}
	
	public static class youtubeMapper extends Mapper<Object, Text, Text, Text> 
	{
		public void map(Object key, Text value, Context context)throws IOException, InterruptedException 
		{
			String record = value.toString();
			String[] parts = record.split(",");
			context.write(new Text(parts[0]), new Text("youtube\t"+parts[1]));
		}
	}


	public static class emailReducer extends Reducer<Text, Text, Text, Text> 
	{
		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException 
		{
			int likes = 0;
			int tweets = 0;
			int views = 0;
			int total = 0;
			for (Text like : values) 
			{
				String parts[] = like.toString().split("\t");
				if (parts[0].equals("facebook")) 
				{
					likes = Integer.parseInt(parts[1]);
				} 
				else if (parts[0].equals("twitter")) 
				{
					tweets = Integer.parseInt(parts[1]);
				}
				else if (parts[0].equals("youtube")) 
				{
					views = Integer.parseInt(parts[1]);
				}
			}
			total = likes+tweets+views;
			String result = "Flag [ Facebook : "+likes+" Twitter : "+tweets+" Youtube : "+views+" ] Total : "+total;
			context.write(new Text(key), new Text(result));
		}
	}

	public static void main(String[] args) throws Exception 
	{
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
	    job.setJarByClass(adDataAnalysis.class);
	    job.setJobName("Reduce Side Join");
		job.setReducerClass(emailReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, facebookMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, twitterMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[2]),TextInputFormat.class, youtubeMapper.class);
		
		Path outputPath = new Path(args[3]);
		FileOutputFormat.setOutputPath(job, outputPath);		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}


