package employee4;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import employee4.EmployeeDetails4;

public class EmployeeDetails4 {public static class MapClass extends Mapper<LongWritable,Text,Text,LongWritable>
{
    public void map(LongWritable key, Text value, Context context)
    {	    	  
       try{
	            String[] str = value.toString().split(",");
	          	 
	            long salary = Long.parseLong(str[3]);
	            context.write(new Text(str[1]),new LongWritable(salary));

         
         //context.write(key,value);
       }
       catch(Exception e)
       {
          System.out.println(e.getMessage());
       }
    }
 }
public static class ReduceClass extends Reducer<Text,LongWritable,Text,DoubleWritable>
{
	   //private LongWritable result = new LongWritable();
	    
	    public void reduce(Text key, Iterable<LongWritable> values,Context context) throws IOException, InterruptedException {
	      long sum = 0;
	      int count=0;
	     double avg=0;
			
	         for (LongWritable total : values)
	         {       	
	        	sum += total.get(); 
	        	count++;
	         }
	        avg=  (int) (sum/count);
	      double ans=(avg+',' +count);
	         
	     //result.set();		      
	      context.write(key,new DoubleWritable(ans));
	      //context.write(key, new LongWritable(sum));
	      
	    }
}

public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    //conf.set("name", "value")
	    //conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
	    Job job = new Job (conf, "Count");
	    job.setJarByClass(EmployeeDetails4.class);
	    job.setMapperClass(MapClass.class);
	    //job.setCombinerClass(ReduceClass.class);
	    job.setReducerClass(ReduceClass.class);
	    //job.setNumReduceTasks(2);
	 //   job.setMapOutputKeyClass(Text.class);
	  //  job.setMapOutputValueClass(LongWritable.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }

}



