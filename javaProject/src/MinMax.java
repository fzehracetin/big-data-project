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

public class MinMax {
	public static class MinMaxMapper extends Mapper<Object, Text, Text, IntWritable> {
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
	
	public static class MinMaxReducer extends Reducer<Text, IntWritable, Text, Text> {
		 private Text result = new Text();
		 public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			 Integer min = 1000, max = 0;
			 
			 for (IntWritable val : values) {
				 if (val.get() < min) {
					 min = val.get();
				 }
				 if (val.get() > max) {
					 max = val.get();
				 }
			 }
			 String res = min.toString() + "," + max.toString();
			 result.set(res);
			 context.write(key, result);
		 }
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "minMax");
		job.setJarByClass(MinMax.class);
		job.setMapperClass(MinMaxMapper.class);
		job.setReducerClass(MinMaxReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
