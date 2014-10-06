import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class WordCount3 {
    public static class WordData implements Comparable<WordData> {
        public Text key;
        public Integer value;

        public WordData(Text k, int v) {
            key = k;
            value = v;
        }

        public int compareTo(WordData other) {
            return other.value - value;
        }

    }
        
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            if (word.getLength() == 7) {
                context.write(word, one);
            }
        }
    }
 
 } 
        
 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    private PriorityQueue<WordData> queue = new PriorityQueue<WordData>();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        WordData wd = new WordData(new Text(key), sum);
        queue.add(wd);
    }

     protected void cleanup(Context context)  throws IOException, InterruptedException {
        Text word = new Text();
        int n = Math.min(100, queue.size());
        for (int i=0; i<n; i++) {
            WordData wd = queue.poll();
            word.set(wd.key);
            context.write(word, new IntWritable(wd.value));
        }
    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        Job job = new Job(conf, "wordcount3");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setNumReduceTasks(1);
    job.setJarByClass(WordCount3.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}