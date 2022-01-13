package SecondStep;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;
import FirstStep.Trigram;
import FirstStep.TrigramLine;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import FirstStep.FirstStepValue;
import FirstStep.FirstStepKey;
import FirstStep.Trigram;
import FirstStep.TrigramLine;

public class SecondStep {

    public static class MapperClass extends Mapper<FirstStepKey, FirstStepValue, FirstStepKey, FirstStepValue> {

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
        }

        @Override
        public void map(FirstStepKey key, FirstStepValue value, Context context) throws IOException, InterruptedException {

        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
        }

    }

    public static class ReducerClass extends Reducer<FirstStepKey, FirstStepValue, SecondStepOutputKey, SecondStepOutputValue> {

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
        // TODO:
        //  might need to do second sorting here according to the tag before word2 (first word1 and than c before i).
        //  another option is to add a new key class with the needed compareTo function
        }

        @Override
        public void reduce(FirstStepKey key, Iterable<FirstStepValue> values, Context context) throws IOException, InterruptedException {

        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
        }
    }

    public static class CombinerClass extends Reducer<FirstStepKey, FirstStepValue, FirstStepKey, FirstStepValue> {

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
        }

        @Override
        public void reduce(FirstStepKey key, Iterable<FirstStepValue> values, Context context) throws IOException, InterruptedException {

        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
        }
    }

    public static class PartitionerClass extends Partitioner<FirstStepKey, FirstStepValue> {

        @Override
        public int getPartition(FirstStepKey key, FirstStepValue value, int numReducers) {
            return key.hashCode() % numReducers;
        }

    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "SecondStep");
        job.setJarByClass(SecondStep.class);

        job.setMapperClass(SecondStep.MapperClass.class);
        job.setPartitionerClass(SecondStep.PartitionerClass.class);
        job.setCombinerClass(SecondStep.CombinerClass.class);
        job.setReducerClass(SecondStep.ReducerClass.class);

        // Set Mapper output format:
        job.setMapOutputKeyClass(FirstStepKey.class);
        job.setMapOutputValueClass(FirstStepValue.class);

        // Set Reducer output format:
        job.setOutputKeyClass(SecondStepOutputKey.class);
        job.setOutputValueClass(SecondStepOutputValue.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}