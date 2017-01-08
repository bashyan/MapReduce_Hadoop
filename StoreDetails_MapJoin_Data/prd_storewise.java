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
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class prd_storewise {
	
	
	public static class MyMapper extends Mapper<LongWritable,Text, Text, Text> {
        
		
		
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		
        protected void map(LongWritable key, Text value, Context context)
            throws java.io.IOException, InterruptedException 
            {
        	
        	
        	String row = value.toString();
        	String[] tokens = row.split(",");
        	outputKey.set(tokens[1]);
        	outputValue.set(tokens[2]+","+tokens[0]); //11, 101, 20, 10, MAH
      	  	context.write(outputKey,outputValue);
        }  
}
	
	public static class myPartitioner extends Partitioner < Text, Text >
	   {
	      @Override
	      public int getPartition(Text key, Text value, int numReduceTasks)
	      {
	         String[] str = value.toString().split(",");
	         
	         String store = str[1];
	         
	         if(store.equals("11"))
	         {
	            return 0;
	         }
	         else if(store.equals("12"))
	         {
	            return 1;
	         }
	         else
	         {
	        	return 2;
	         }
	         
	      }
	   }
	
	public static class myReducer extends Reducer<Text,Text,Text, IntWritable>
	   {
	      
	      private Text resultKey = new Text();
	      private IntWritable resultValue = new IntWritable();
	      
	      public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	      {
	    	  
	    	  int qty = 0;
	    	  for (Text val : values)
	          {
	    		  resultKey.set(key);
	    		  String[] row = val.toString().split(",");
	    		  qty += Integer.parseInt(row[0]);
	    		 	    		  
	          }
	    	  
	    	  resultValue.set(qty);
	    	  context.write(resultKey, resultValue);
	    	  
	      }     
	      
	   }
	
	
  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
  {
    
	Configuration conf = new Configuration();
	//conf.set("mapred.textoutputformat.separator", ",");
	Job job = Job.getInstance(conf);
    job.setJarByClass(prd_storewise.class);
    job.setJobName("Store Wise");
    job.setMapperClass(MyMapper.class);
    job.setPartitionerClass(myPartitioner.class);
    job.setReducerClass(myReducer.class);
    job.setNumReduceTasks(3);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.waitForCompletion(true);
    
    }
}