import java.io.IOException;  
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;

public class sale_storewise 
{
	
	public static class MyMapper extends Mapper<LongWritable,Text, Text, Text> 
	{
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		
        public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException 
            {        	
        	String row = value.toString();
        	String[] tokens = row.split(",");
        	outputKey.set(tokens[0]);
        	outputValue.set(tokens[2]+","+tokens[3]); //11, 101, 20, 10, MAH
      	  	context.write(outputKey,outputValue);
        }  
	}	
	
	
	public static class myReducer extends Reducer<Text,Text,Text, IntWritable>
	   {	      
	      private IntWritable resultValue = new IntWritable();
	      
	      public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	      {
	    	  int tot_sale = 0;
	    	  for (Text val : values)
	          {
	    		  String[] rows = val.toString().split(",");
	    		  tot_sale += Integer.parseInt(rows[0]) * Integer.parseInt(rows[1]);
	          }	    	  
	    	  resultValue.set(tot_sale);
	    	  context.write(key, resultValue);	    	  
	      }     	      
	   }
	
	
  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
  {
    
	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf);
    job.setJarByClass(sale_storewise.class);
    job.setJobName("Store Wise");
    job.setMapperClass(MyMapper.class);
    //job.setNumReduceTasks(0);
    //job.setCombinerClass(myReducer.class);
    job.setReducerClass(myReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    
    }
}