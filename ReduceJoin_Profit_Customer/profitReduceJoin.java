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

public class profitReduceJoin {

	public static class prdMapper extends
			Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String record = value.toString();
			String[] parts = record.split(",");
			context.write(new Text(parts[0]), new Text("p#\t" + parts[2]));
		}
	}

	public static class saleMapper extends
			Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String record = value.toString();
			String[] parts = record.split(",");
			context.write(new Text(parts[0]), new Text("s#\t" + parts[1] +"\t" + parts[2]));
		}
	}

	public static class ReduceJoinReducer extends Reducer<Text, Text, Text, Text> {
		
		Text outputval = new Text();
		String str = "";
		
		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException
		{
			float cp = 0, cp1 = 0;
			float sp = 0, sp1 = 0;
			float sale_qty = 0, sale_qty1 = 0;
			float profit = 0, profit1 = 0, profit2 = 0;
			for(Text val : values)
			{
				
				String parts[] = val.toString().split("\t");
				if (parts[0].equals("p#")) {
					
					cp = cp1;
					cp1 = Float.parseFloat(parts[1]);
					
					
				} else if (parts[0].equals("s#")) {
					
					sale_qty = sale_qty1;
					sale_qty1 = Float.parseFloat(parts[1]);
					sp = sp1;
					sp1 = Float.parseFloat(parts[2]);					
					
				}				
			}
			profit1 = sale_qty * (sp - cp);
			profit2 = sale_qty1 * (sp1 - cp1);
			profit = profit1 + profit2;
			String str = "Profit: "+ Float.toString(profit);
			context.write(key, new Text(str));
		}
		
	}

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
	    job.setJarByClass(profitReduceJoin.class);
	    job.setJobName("Reduce Side Join");
	    //job.setCombinerClass(ReduceJoinReducer.class);
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