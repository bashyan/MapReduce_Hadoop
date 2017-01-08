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

public class prdReduceJoin {

	public static class prdMapper extends
			Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String record = value.toString();
			String[] parts = record.split(",");
			context.write(new Text(parts[0]), new Text("p#\t" + parts[1]));
		}
	}

	public static class saleMapper extends
			Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String record = value.toString();
			String[] parts = record.split(",");
			context.write(new Text(parts[0]), new Text("s#\t" + parts[1]));
		}
	}

	public static class ReduceJoinReducer extends Reducer<Text, Text, Text, Text> {
		
		Text outputval = new Text();
		String str = "";
		String prd = "";
		String sale = "";
		
		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException
		{
			int total1 = 0;
			int total2 = 0;
			for(Text val : values)
			{
				
				String parts[] = val.toString().split("\t");
				if (parts[0].equals("p#")) {
					total1 += Integer.parseInt(parts[1]);
					prd = Integer.toString(total1);
				} else if (parts[0].equals("s#")) {
					total2 += Integer.parseInt(parts[1]);
					sale = Integer.toString(total2);
					
				}
			}
			String str = "Produced"+":"+prd+" Sold"+":"+sale;
			context.write(key, new Text(str));
		}
		
	}

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
	    job.setJarByClass(prdReduceJoin.class);
	    job.setJobName("Reduce Side Join");
		job.setReducerClass(ReduceJoinReducer.class);
		//job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, prdMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, saleMapper.class);
		
		Path outputPath = new Path(args[2]);
		FileOutputFormat.setOutputPath(job, outputPath);
		//outputPath.getFileSystem(conf).delete(outputPath);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}