

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class telephoneMap {

  public static class myMapper extends Mapper<LongWritable, Text, Text, LongWritable>
  {
	  public void map(LongWritable key, Text value, Context context)
	  {
		  try
		  {
			  String[] str = value.toString().split(",");
			  String start_time = str[2];
			  String end_time = str[3];
			  
			  
			  
			  SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	          Date Frm = null;
	          Date To = null;
	          try 
	          {
	              Frm = format.parse(start_time);
	              To = format.parse(end_time);
	          } 
	          catch (ParseException e)
	          {
	              e.printStackTrace();
	          }
	            
	          long diff = To.getTime() - Frm.getTime();
	          //long diffSeconds = (diff/1000) % 60;
	          long diffMinutes = (diff/1000) / 60;
			  
	          if(str[4].equals("1"))
	          {
	        	  context.write(new Text(str[0]), new LongWritable(diffMinutes) );	        	  
	          }		  
		  }		  
		  catch(Exception e)
		  {
			  e.printStackTrace();
		  }
	  }
  }
	  

  public static class myReducer extends Reducer<Text,LongWritable,Text,LongWritable> 
  {
    //private LongWritable result = new LongWritable();

    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
    {
    	for(LongWritable minutes : values)
    	{
    		long minute = minutes.get();
    		if(minute>60)
    		{
    			context.write(key, minutes);
    		}
    	}
    }
  }

  public static void main(String[] args) throws Exception 
  {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Volume count");
    job.setJarByClass(telephoneMap.class);
    job.setMapperClass(myMapper.class);
    job.setCombinerClass(myReducer.class);
    job.setReducerClass(myReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}