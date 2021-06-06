import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class StandardDeviation {
	
	public static class StandardDeviationMapper extends Mapper<Object, Text, Text, IntWritable> {
		 private IntWritable rate = new IntWritable();
		 private Text movie = new Text();
		 public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			 String[] lines = value.toString().split(System.getProperty("line.separator"));
	
	        for (String line: lines) {
	        	String[] tokens = line.split(",");
	        	movie.set(tokens[3]);
	        	rate.set(Integer.parseInt(tokens[1]));
	        	try {
					context.write(movie, rate);
				} 
	        	catch (IOException | InterruptedException e) {
					e.printStackTrace();
				}
	        }
		 }
	}
	
	public static class StandardDeviationReducer extends Reducer<Text, IntWritable, Text, FloatWritable> {
		 private FloatWritable result = new FloatWritable();
		 public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			 int sum = 0, n = 0;
			 
			 ArrayList <Integer> valueList = new ArrayList<Integer>();
			 for (IntWritable val : values) {
				 valueList.add(val.get());
				 sum += val.get();
				 n++;
			 }
			 
			 Float avg = (float) sum / n;
			 
			 Float std = 0f, diff = 0f;
			 for (Integer val : valueList) {
				  diff += (float) Math.pow(avg - val, 2);	  
			 }
			 
			 std = (float) Math.sqrt(diff / n); 
			 result.set(std);
			 context.write(key, result);
	
		 }
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "standardDeviation");
		job.setJarByClass(StandardDeviation.class);
		job.setMapperClass(StandardDeviationMapper.class);
		job.setReducerClass(StandardDeviationReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}


}
