package student;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import student.StudentData;


public class StudentData 
{
	public static class MapClass extends Mapper<LongWritable,Text,Text,LongWritable>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	         try{
	            //String[] str = value.toString().split(",");	 
	            //long vol = Long.parseLong(str[5]);
	            context.write(value,key);
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
	
	      public static void main(String[] args) throws Exception {
			    Configuration conf = new Configuration();
			    //conf.set("name", "value")
			    //conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
			    Job job = new Job (conf, "Count");
			    job.setJarByClass(StudentData.class);
			    job.setMapperClass(MapClass.class);
			    //job.setCombinerClass(ReduceClass.class);
			    //job.setReducerClass(ReduceClass.class);
			    //job.setNumReduceTasks(2);
			    job.setOutputKeyClass(Text.class);
			    job.setOutputValueClass(LongWritable.class);
			    FileInputFormat.addInputPath(job, new Path(args[0]));
			    FileOutputFormat.setOutputPath(job, new Path(args[1]));
			    System.exit(job.waitForCompletion(true) ? 0 : 1);
			  }

	}

	   
 

      

	
	
	
	
	
	
	
	

