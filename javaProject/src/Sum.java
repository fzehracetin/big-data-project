import java.io.IOException;
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

public class Sum {
	
	public static class SumMapper extends Mapper<Object, Text, Text, IntWritable> {
		 private final static IntWritable one = new IntWritable(1);
		 private Text date = new Text();
		 
		 public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			 String[] lines = value.toString().split(System.getProperty("line.separator"));
	
	        for (String line: lines) {
	        	String[] tokens = line.split(",");
	        	date.set(tokens[2]);
	        	try {
					context.write(date, one);
				} 
	        	catch (IOException | InterruptedException e) {
					e.printStackTrace();
				}
	        }
		 }
	}
	
	public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		 private IntWritable result = new IntWritable();
		 public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			 int sum = 0;
			 for (IntWritable val : values) {
				 sum += val.get();
			 }
			 result.set(sum);
			 context.write(key, result);
		 }
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "sum");
		job.setJarByClass(Sum.class);
		job.setMapperClass(SumMapper.class);
		job.setReducerClass(SumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
