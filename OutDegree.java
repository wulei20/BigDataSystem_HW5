import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class OutDegree
{

	public static class OutDegreeMapper 
		extends Mapper<Object, Text, Text, IntWritable>
	{
    
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
      
		public void map(Object key, Text value, Context context
							) throws IOException, InterruptedException 
		{
			// Split the input line
            StringTokenizer itr = new StringTokenizer(value.toString());
            if (itr.hasMoreTokens()) {
                String prefix = itr.nextToken(); // The 'a' prefix
                if (prefix.equals("a") && itr.hasMoreTokens()) {
                    String u = itr.nextToken(); // The starting node
                    word.set(u);
                    context.write(word, one);
                }
            }
		}
	}
  
	public static class OutDegreeReducer 
		extends Reducer<Text,IntWritable,Text,IntWritable> 
	{
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, 
									Context context
							) throws IOException, InterruptedException 
		{
			int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) 
		{
			System.err.println("Usage: outdegree <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "outdegree");
		job.setJarByClass(OutDegree.class);
		job.setMapperClass(OutDegreeMapper.class);
		job.setCombinerClass(OutDegreeReducer.class);
		job.setReducerClass(OutDegreeReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) 
		{
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job,
				new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

