import java.io.IOException;
import java.util.PriorityQueue;
import java.util.StringTokenizer;
import java.util.Vector;

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

public class TopKOutDegree {

    public static class OutDegreeMapper 
        extends Mapper<Object, Text, Text, IntWritable> {
        
        private final static IntWritable one = new IntWritable(1);
        private Text vertexKey = new Text();

        public void map(Object key, Text value, Context context) 
                throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            if (itr.hasMoreTokens()) {
                String prefix = itr.nextToken(); // The 'a' prefix
                if (prefix.equals("a") && itr.hasMoreTokens()) {
                    String u = itr.nextToken(); // The starting node
                    vertexKey.set(u);
                    context.write(vertexKey, one);
                }
            }
        }
    }

    public static class TopKReducer 
        extends Reducer<Text, IntWritable, Text, IntWritable> {
        
        private PriorityQueue<VertexDegree> queue;
        private Vector<VertexDegree> topK;
        private int k;

        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            k = conf.getInt("top.k", 1); // Default to top-1 if not set
            queue = new PriorityQueue<>(k);
            topK = new Vector<>();
        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            queue.add(new VertexDegree(key.toString(), sum));
            if (queue.size() > k) {
                queue.poll();
            }
        }

        protected void cleanup(Context context) 
                throws IOException, InterruptedException {
            while (!queue.isEmpty()) {
                VertexDegree vd = queue.poll();
                topK.add(vd);
            }
            for (int i = topK.size() - 1; i >= 0; i--) {
                VertexDegree vd = topK.get(i);
                context.write(new Text(vd.vertex), new IntWritable(vd.degree));
            }
        }
    }

    public static class VertexDegree implements Comparable<VertexDegree> {
        String vertex;
        int degree;

        VertexDegree(String vertex, int degree) {
            this.vertex = vertex;
            this.degree = degree;
        }

        public int compareTo(VertexDegree other) {
            return Integer.compare(this.degree, other.degree);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: topkoutdegree <in> <out> <k>");
            System.exit(2);
        }
        
        // Set the top-k value
        int k = 1;
        if (otherArgs.length > 2) {
            k = Integer.parseInt(otherArgs[2]);
        }
        conf.setInt("top.k", k);

        // Job: Calculate Out-Degree and find Top-K Nodes
        Job job = Job.getInstance(conf, "topkoutdegree");
        job.setJarByClass(TopKOutDegree.class);
        job.setMapperClass(OutDegreeMapper.class);
        job.setReducerClass(TopKReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
