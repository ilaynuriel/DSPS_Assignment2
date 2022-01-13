package FirstStep;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class FirstStep {

    public static class MapperClass extends Mapper<LongWritable, Text, FirstStepKey, FirstStepValue> {

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            TrigramLine trigramLine = new TrigramLine(value);
            // For C0 calculation:
            context.write(
                    new FirstStepKey("*", "*", "*", 'c'),
                    new FirstStepValue(trigramLine.getOccurrences().get())
            );
            // For counting w1, <w1, w2> bigram, <w1, w2, w3> trigram, occurrences:
            context.write(
                    new FirstStepKey(trigramLine.getTrigram().getW1(), "*", "*", 'c'),
                    new FirstStepValue(trigramLine.getOccurrences().get())
            );
            context.write(
                    new FirstStepKey(trigramLine.getTrigram().getW1(), trigramLine.getTrigram().getW2(), "*", 'c'),
                    new FirstStepValue(trigramLine.getOccurrences().get())
            );
            context.write(
                    new FirstStepKey(trigramLine.getTrigram().getW1(), trigramLine.getTrigram().getW2(), trigramLine.getTrigram().getW3(), 'c'),
                    new FirstStepValue(trigramLine.getOccurrences().get())
            );
            // For calculating the probability in the next M-R step:
            context.write(
                    new FirstStepKey(trigramLine.getTrigram().getW2(), "*", trigramLine.getTrigram().getW1(), 'i'),
                    new FirstStepValue((long) -1)
            );
            context.write(
                    new FirstStepKey(trigramLine.getTrigram().getW2(), trigramLine.getTrigram().getW3(), trigramLine.getTrigram().getW1(), 'i'),
                    new FirstStepValue((long) -1)
            );
            context.write(
                    new FirstStepKey(trigramLine.getTrigram().getW3(), "*", trigramLine.getTrigram().getW1(), 'i'),
                    new FirstStepValue((long) -1)
            );
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
        }

    }

    public static class ReducerClass extends Reducer<FirstStepKey, FirstStepValue, FirstStepKey, FirstStepValue> {
        char prevKeyTag;
        long word1Count;
        long word1word2Count;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            prevKeyTag = 'x';
            word1Count = -1;
            word1word2Count = -1;
        }

        @Override
        public void reduce(FirstStepKey key, Iterable<FirstStepValue> values, Context context) throws IOException, InterruptedException {
            // Here we assume the order of the keys is according to the FirstStepKey compareTo() function.
            // for each word1 in key, the order will be:
            // word1, wordx, c, ...
            // ...
            // word1, wordx, i, ...
            // word1, wordy, c, ...
            // ...
            // word1, wordy, i, ...
            // Therefore we initialize a small memory to save temporary values.

            if (key.getTag().charAt(0) == 'c') {
                if (this.prevKeyTag == 'i') {
                    // TODO: not sure we need this part...
                    // Emit all lines in memory - Done bellow?
                    // "Delete" memory?
                }
                if (this.prevKeyTag != 'c')
                    this.prevKeyTag = 'c';
                long sum = 0;
                for (FirstStepValue value : values)
                    sum += value.getCount().get();
                if (key.getWord3().charAt(0) == '*') {
                    if (key.getWord2().charAt(0) == '*') {
                        if (key.getWord1().charAt(0) == '*') {
                            context.write(
                                    new FirstStepKey(key.getWord1().toString(), key.getWord2().toString(), key.getWord3().toString(), 'c'),
                                    new FirstStepValue(sum)
                            );
                        } else {
                            // emit sum - it counted all word1 instances
                            context.write(
                                    new FirstStepKey(key.getWord1().toString(), key.getWord2().toString(), key.getWord3().toString(), 'c'),
                                    new FirstStepValue(sum)
                            );
                            // save sum in word1Count
                            this.word1Count = sum;
                        }
                    } else {
                        // emit sum - it counted all <word1, word2> instances
                        context.write(
                                new FirstStepKey(key.getWord1().toString(), key.getWord2().toString(), key.getWord3().toString(), 'c'),
                                new FirstStepValue(sum)
                        );
                        // save sum in word1word2Count
                        this.word1word2Count = sum;
                    }
                } else {
                    context.write(
                            new FirstStepKey(key.getWord1().toString(), key.getWord2().toString(), key.getWord3().toString(), 'c'),
                            new FirstStepValue(sum)
                    );
                }
            } else if (key.getTag().charAt(0) == 'i') {
                if (this.prevKeyTag == 'c') {
                    this.prevKeyTag = 'i';
                }
                long val2emit = -1;
                // Note: when a key is i tagged - word1, word3 should never be equal to *
                if (key.getWord1().charAt(0) == '*' || key.getWord3().charAt(0) == '*') {
                    // TODO: Error
                }
                else {
                    if (key.getWord2().charAt(0) == '*') {
                        // take value from word1Count
                        val2emit = this.word1Count;

                    } else {
                        // take value from word1word2Count
                        val2emit = this.word1word2Count;
                    }
                }
                // We dont iterate over values in this case because we should emit only one value per key - We just send
                // new information here for the next step:
                // We change order inside the key from <word1, word2, word3> to <word3, word1, word2>, so the reducer in
                // the next step will get all info it need to compute the trigram <word3, word1, word2>:
                context.write(
                        new FirstStepKey(key.getWord3().toString(), key.getWord1().toString(), key.getWord2().toString(), 'i'),
                        new FirstStepValue(val2emit)
                );
            }
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
            if (key.getTag().charAt(0) == 'c') {
                long sum = 0;
                for (FirstStepValue value : values)
                    sum += value.getCount().get();
                context.write(key, new FirstStepValue(sum));
            } else if (key.getTag().charAt(0) == 'i') {
                if (values.iterator().hasNext()) {
                    context.write(key, values.iterator().next());
                }
            }
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

        Job job = Job.getInstance(conf, "FirstStep");
        job.setJarByClass(FirstStep.class);

        job.setMapperClass(MapperClass.class);
        //job.setPartitionerClass(PartitionerClass.class);
        //job.setCombinerClass(CombinerClass.class);
        job.setReducerClass(ReducerClass.class);

        //job.setMapOutputKeyClass(K2.class);
        //job.setMapOutputValueClass(K2.class);
        //job.setOutputKeyClass(K3.class);
        //job.setOutputValueClass(K3.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setInputFormatClass(TextInputFormat.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}