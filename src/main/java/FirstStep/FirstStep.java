package FirstStep;

import java.io.IOException;

import SecondStep.SecondStepKey;
import SecondStep.SecondStepValue;
import jdk.nashorn.internal.parser.JSONParser;
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
            String[] trigramCheck = value.toString().split("\t")[0].split(" ");
            if (isValid(trigramCheck)) {
                TrigramLine trigramLine = new TrigramLine(value);
                // For C0 calculation:
                context.write(
                        new FirstStepKey("!", "!", "!", 'c'),
                        new FirstStepValue(new Trigram(), trigramLine.getOccurrences().get())
                );
                // For counting w1, <w1, w2> bigram, <w1, w2, w3> trigram, occurrences:
                context.write(
                        new FirstStepKey(trigramLine.getTrigram().getW1(), "!", "!", 'c'),
                        new FirstStepValue(new Trigram(trigramLine.getTrigram().getW1(), "!", "!"), trigramLine.getOccurrences().get()) // TODO should we send the trigram? its being overwritten in the combiner
                );
                context.write(
                        new FirstStepKey(trigramLine.getTrigram().getW1(), "!", "!", 'i'),
                        new FirstStepValue(trigramLine.getTrigram(), (long) -1)
                );
                context.write(
                        new FirstStepKey(trigramLine.getTrigram().getW1(), trigramLine.getTrigram().getW2(), "!", 'c'),
                        new FirstStepValue(new Trigram(trigramLine.getTrigram().getW1(), trigramLine.getTrigram().getW2(), "!"), trigramLine.getOccurrences().get()) // TODO should we send the trigram? its being overwritten in the combiner
                );
                context.write(
                        new FirstStepKey(trigramLine.getTrigram().getW1(), trigramLine.getTrigram().getW2(), "!", 'i'),
                        new FirstStepValue(trigramLine.getTrigram(), (long) -1)
                );
                context.write(
                        new FirstStepKey(trigramLine.getTrigram().getW1(), trigramLine.getTrigram().getW2(), trigramLine.getTrigram().getW3(), 'c'),
                        new FirstStepValue(trigramLine.getTrigram(), trigramLine.getOccurrences().get()) // TODO should we send the trigram? its being overwritten in the combiner
                );
                // For calculating the probability in the next M-R step:
                context.write(
                        new FirstStepKey(trigramLine.getTrigram().getW2(), "!", "!" /*trigramLine.getTrigram().getW1()*/, 'i'), // TODO not sure we need w1
                        new FirstStepValue(trigramLine.getTrigram(), (long) -1)
                );
                context.write(
                        new FirstStepKey(trigramLine.getTrigram().getW2(), trigramLine.getTrigram().getW3(), "!" /*trigramLine.getTrigram().getW1()*/, 'i'), // TODO not sure we need w1
                        new FirstStepValue(trigramLine.getTrigram(), (long) -1)
                );
                context.write(
                        new FirstStepKey(trigramLine.getTrigram().getW3(), "!", "!" /*trigramLine.getTrigram().getW1()*/, 'i'), // TODO not sure we need w1
                        new FirstStepValue(trigramLine.getTrigram(), (long) -1)
                );
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
        }

        private boolean isValid(String[] gram) {
            return gram.length == 3 && isValid(gram[0]) && isValid(gram[1]) && isValid(gram[2]);
        }

        private boolean isValid(String word) {
            return word.matches("^[\\u0590-\\u05ff]+$");
        }
    }

    public static class ReducerClass extends Reducer<FirstStepKey, FirstStepValue, SecondStepKey, SecondStepValue> {
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

                if (key.getWord3().charAt(0) == '!') {
                    if (key.getWord2().charAt(0) == '!') {
                        if (key.getWord1().charAt(0) == '!') {
                            context.write(
                                    // key = <<!, !, !>, !, !, c>
                                    new SecondStepKey(new Trigram(),'c'),
                                    new SecondStepValue(sum)
                            );
                        } else {
                            // save sum in word1Count
                            this.word1Count = sum;
//                            // emit sum - it counted all word1 instances
//                            context.write(
//                                    // key = <<word1, !, !>, !, !, c>
//                                    new SecondStepKey(new Trigram(key.getWord1().toString(), key.getWord2().toString(), key.getWord3().toString()),'c'),
//                                    new SecondStepValue(sum)
//                            );
                        }
                    } else {
                        // save sum in word1word2Count
                        this.word1word2Count = sum;
//                        // emit sum - it counted all <word1, word2> instances
//                        context.write(
//                                // key = <<word1, word2, !>, !, !, c>
//                                new SecondStepKey(new Trigram(key.getWord1().toString(), key.getWord2().toString(), key.getWord3().toString()),'c'),
//                                new SecondStepValue(sum)
//                        );
                    }
                } else {
                    context.write(
                            // key = <<word1, word2, word3>, !, !, c>
                            new SecondStepKey(new Trigram(key.getWord1().toString(), key.getWord2().toString(), key.getWord3().toString()), 'c'),
                            new SecondStepValue(sum)
                    );
                }
            } else if (key.getTag().charAt(0) == 'i') {
                if (this.prevKeyTag == 'c') {
                    this.prevKeyTag = 'i';
                }

                // when a key is i tagged - word1, word3 should never be equal to ! - Note not true because of my recent change
//                if (key.getWord1().charAt(0) == '!' || key.getWord3().charAt(0) == '!') {
//                    // Error
//                } else {
                    for (FirstStepValue value : values) {
                        long val2emit = -1;
                        if (key.getWord2().charAt(0) == '!') {
                            // take value from word1Count
                            val2emit = this.word1Count;
                        } else {
                            // take value from word1word2Count
                            val2emit = this.word1word2Count;
                        }
                        context.write(
                                // key = <Trigram, word1, word2(might be !), i>
                                new SecondStepKey(value.getTrigram(), key.getWord1().toString(), key.getWord2().toString(), 'i'),
                                new SecondStepValue(val2emit)
                        );
                    }
                //}
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
                context.write(key, new FirstStepValue(new Trigram(key.getWord1().toString(), key.getWord2().toString(), key.getWord3().toString()), sum));
            } else if (key.getTag().charAt(0) == 'i') {
                // if w2 and w3 aren't equal to ! so we know that the Trigram in value is also the same for all values for this key.
                if (key.getWord2().toString() != "!" && key.getWord3().toString() != "!"){
                    context.write(key, values.iterator().next());
                }
                else
                {
                    // TODO: not sure how do i combine here when w2 = "!" or w3 = "!" (or both), it depends also on the Trigram in the value...
                    for (FirstStepValue value : values)
                        context.write(key, value);
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
            System.out.print("\nEntered First Step getPartition. numReducers = " + numReducers +" Key Hash = " + key.hashCode());
            return Math.abs(key.hashCode() % numReducers);
        }

    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "FirstStep");
        job.setJarByClass(FirstStep.class);

        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(CombinerClass.class);
        job.setReducerClass(ReducerClass.class);

        // Set Mapper output format:
        job.setMapOutputKeyClass(FirstStepKey.class);
        job.setMapOutputValueClass(FirstStepValue.class);

        // Set Reducer output format:
        job.setOutputKeyClass(SecondStepKey.class);
        job.setOutputValueClass(SecondStepValue.class);

        job.setInputFormatClass(TextInputFormat.class); // TextInputFormat?
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}