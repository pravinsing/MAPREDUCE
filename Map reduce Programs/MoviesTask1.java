package moviesinfo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import moviesinfo.MoviesInfo;


public class MoviesInfo
{
public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
{
    public void map(LongWritable key, Text value, Context context)
    {	  
    	
       try{
	            String[] str = value.toString().split(",");
	          	 
	            long year = Long.parseLong(str[2]);
	            if(year>1945 && year<=1959)
	            
	            {
	            	String moviesname=str[1];
	            	context.write(new Text("1"),new Text(moviesname));
	          
	            }
         //context.write(key,value);
       }
       catch(Exception e)
       {
          System.out.println(e.getMessage());
       }
    }
 }

public static class ReduceClass extends Reducer<Text,Text,Text,IntWritable>
{
	   //private LongWritable result = new LongWritable();
	    
	    public void reduce(Text inkey, Iterable<Text> inval,Context context) throws IOException, InterruptedException {
	     
	      int count=0;
	    
			
	         for (@SuppressWarnings("unused") Text V:inval)
	         {   
	      
	        	count++;
	 
	         }
	      
	         
	     //result.set();		      
	      context.write(inkey,new IntWritable(count));
	      //context.write(key, new LongWritable(sum));
	      
	    }
}


public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    //conf.set("name", "value")
	    //conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
	    Job job = new Job (conf, "Count");
	    job.setJarByClass(MoviesInfo.class);
	    job.setMapperClass(MapClass.class);
	    //job.setCombinerClass(ReduceClass.class);
	    job.setReducerClass(ReduceClass.class);
	    //job.setNumReduceTasks(2);
	  job.setMapOutputKeyClass(Text.class);
	   job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }

}





