package book;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * This Project Book Usecase - FInd Feq of books sold in every year with NEW API
 */
public class BookFrequency {
	
	public static class BookMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private static final IntWritable one = new IntWritable(1);
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			//String line_temp = value.toString().replaceAll("&amp;", " ");
			String line_temp = value.toString();
			String[] line = line_temp.split("\";\"");	
			if (!line[3].equals("Year-Of-Publication")){
			context.write(new Text(line[3]), one);
			}
		}
		
	}

	public static class BookReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
			
			int freqYear = 0;
			
			for(IntWritable value : values){
				freqYear = freqYear + value.get();
			} 
			System.out.println("Freq = :"+freqYear);
			context.write(key, new IntWritable(freqYear));
		}
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "BookFrequency");
		job.setJarByClass(BookFrequency.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(BookMapper.class);
		job.setReducerClass(BookReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);	
		job.setNumReduceTasks(1);
		
		//job.setMapOutputKeyClass(Text.class);
		//job.setMapOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		System.out.println("Assignment 1 : Book Frequency per year Job Complete");
	}

}
