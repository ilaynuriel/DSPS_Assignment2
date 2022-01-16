package SecondStep;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import java.util.*;


public class SecondStep {

    public static class MapperClass extends Mapper<LongWritable, Text, SecondStepKey, SecondStepValue> {

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.print("Key = " + key.toString() + ", Value = " + value.toString());
            String[] data = value.toString().split("\t");
            String[] gram = data[0].split(" ");
            System.out.print("Data = " + Arrays.toString(data) + ", Trigram = " + Arrays.toString(gram));
            SecondStepKey secondStepKey = new SecondStepKey(new Trigram(gram), data[1], data[2], data[3].charAt(0));
            SecondStepValue secondStepValue = new SecondStepValue(Long.parseLong(data[4]));
            // need to handle sending the C0 value to all reducers
            if (secondStepKey.getTrigram().compareTo(new Trigram()) == 0) { // num of words in corpus
                int numOfReducers = context.getNumReduceTasks();
                for (int i = 0; i < numOfReducers; i++) {
                    context.write(
                            new SecondStepKey(secondStepKey.getTrigram(), String.valueOf(i), "!", 'c'),
                            new SecondStepValue(secondStepValue.getCount().get())
                    );
                }
            }
            else {
                context.write(secondStepKey, secondStepValue);
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
        }

    }

    public static class ReducerClass extends Reducer<SecondStepKey, SecondStepValue, SecondStepOutputKey, SecondStepOutputValue> {

        char prevKeyTag;
        long totalWordCount;
        HashMap<String, Long> wordsCount;
        HashMap<String, Long> pairsCount;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            prevKeyTag = 'x';
            wordsCount = new HashMap<String, Long>();
            pairsCount = new HashMap<String, Long>();
            totalWordCount = -1;
        }

        @Override
        public void reduce(SecondStepKey key, Iterable<SecondStepValue> values, Context context) throws IOException, InterruptedException {
            if (key.getTag().charAt(0) == 'i') {
                wordsCount.clear();
                pairsCount.clear();
                for (SecondStepValue value : values) {
                    if (key.getWord2().toString().equals("!")) // this is information regarding 1 words and not pair
                        wordsCount.put(key.getWord2().toString(), values.iterator().next().getCount().get()); // the count of word1
                    else
                        pairsCount.put(key.getWord1().toString() + " " + key.getWord2().toString(), values.iterator().next().getCount().get());
                }
            } else if (key.getTag().charAt(0) == 'c') { // tag is c
                if (key.getTrigram().compareTo(new Trigram()) == 0)
                    totalWordCount = values.iterator().next().getCount().get(); // the total words in corpus
                else {
                    long word1word2word3Count = values.iterator().next().getCount().get();
                    long word2Count = wordsCount.get(key.getTrigram().getW2());
                    long word3Count = wordsCount.get(key.getTrigram().getW3());
                    long word2word3Count = pairsCount.get(key.getTrigram().getW2() + " " + key.getTrigram().getW3());
                    long word1word2Count = pairsCount.get(key.getTrigram().getW1() + " " + key.getTrigram().getW2());
                    double k2 = Math.log(word2word3Count + 1) + 1 / Math.log(word2word3Count + 1) + 2;
                    double k3 = Math.log(word1word2word3Count + 1) + 1 / Math.log(word1word2word3Count + 1) + 2;
                    double probability =
                            k3 * word1word2word3Count / word1word2Count +
                                    (1 - k3) * k2 * word2word3Count / word2Count +
                                    (1 - k3) * (1 - k2) * word3Count / totalWordCount;
                    context.write(
                            // key = <Trigram, word1, word2(might be !), i>
                            new SecondStepOutputKey(key.getTrigram(), probability),
                            new SecondStepOutputValue(probability)
                    );
                }
            } else {
                // Error tag is not c or i
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
        }
    }

    public static class CombinerClass extends Reducer<SecondStepKey, SecondStepValue, SecondStepKey, SecondStepValue> {

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
        }

        @Override
        public void reduce(SecondStepKey key, Iterable<SecondStepValue> values, Context context) throws IOException, InterruptedException {
            // TODO
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
        }
    }

    public static class PartitionerClass extends Partitioner<SecondStepKey, SecondStepValue> {

        @Override
        public int getPartition(SecondStepKey key, SecondStepValue value, int numReducers) {
            if (key.getTrigram().compareTo(new Trigram()) == 0) { // Its the counting of all words, C0
                //if (isNumeric(key.getWord1().toString()))
                    return Integer.parseInt(key.getWord1().toString());
            }
            else
                return key.hashCode() % numReducers;
        }

        public static boolean isNumeric(String str) {
            try {
                Integer.parseInt(str);
                return true;
            } catch (NumberFormatException e) {
                return false;
            }
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
        job.setMapOutputKeyClass(SecondStepKey.class);
        job.setMapOutputValueClass(SecondStepValue.class);

        // Set Reducer output format:
        job.setOutputKeyClass(SecondStepOutputKey.class);
        job.setOutputValueClass(SecondStepOutputValue.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}