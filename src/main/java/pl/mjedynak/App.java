package pl.mjedynak;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class App {
    public static void main(String[] args) throws Exception {
        Job job = new Job();
        job.setJarByClass(App.class);

        FileInputFormat.addInputPath(job, new Path("src/main/resources"));
        FileOutputFormat.setOutputPath(job, new Path("results/output-" + System.currentTimeMillis()));

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
