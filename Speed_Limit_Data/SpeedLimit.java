

import java.io.IOException;  
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class SpeedLimit 
{

 
	
	public static class myMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>
	  {
		  public void map(LongWritable key, Text value, Context context)
		  {
			  try{
				  String[] str = value.toString().split(",");
				  double vol = Long.parseLong(str[1]);
				  context.write(new Text(str[0]), new DoubleWritable(vol));
			  }
			  catch(Exception e)
			  {
				  e.printStackTrace();
			  }
		  }
	  }
	  

  public static class myReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> 
  {
    private DoubleWritable result = new DoubleWritable();

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException 
    {
    	double totcount = 0, offencount = 0;
    	for(DoubleWritable count : values)
    	{
    		totcount++;
    		if(count.get() > 65)
    		{
    			offencount++;
    		}
    		
    	}
    	double per = ((offencount*100)/totcount);
	      result.set(per);
	      context.write(key,result);
      
    }
  }

  public static void main(String[] args) throws Exception 
  {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Offensive Percentage");
    job.setJarByClass(SpeedLimit.class);
    job.setMapperClass(myMapper.class);
    //job.setCombinerClass(myReducer.class);
    job.setReducerClass(myReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}