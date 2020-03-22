import java.io.IOException;
import java.util.StringTokenizer;
import java.io.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Problem2 {

    public static class TokenizerMapper
            extends Mapper<LongWritable, Text, Text, FloatWritable> {
    	
        private Text videoId = new Text();
        private FloatWritable ratings = new FloatWritable();

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
        	String singleLine = value.toString();
        	String col[] = singleLine.split("\t");
        	if(col.length > 7){
        		videoId.set(col[0]);
                if(col[6].matches("\\d+.+")){
                	float rating = Float.parseFloat(col[6]);
                	ratings.set(rating);
                }
                context.write(videoId, ratings);
        	}
        }
    }

    public static class AvgRatingsReducer
            extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        private FloatWritable result = new FloatWritable();

        public void reduce(Text key, Iterable<FloatWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            float sum = 0;
            int count = 0;
            for (FloatWritable val : values) {
            	count += 1;
                sum += val.get();
            }
            sum = sum/count;
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Problem 2");
        job.setJarByClass(Problem2.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(AvgRatingsReducer.class);
        job.setReducerClass(AvgRatingsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
