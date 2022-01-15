package FirstStep;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Trigram implements WritableComparable<Trigram> {

    private final Text w1;
    private final Text w2;
    private final Text w3;

    public Trigram() {
        w1 = new Text("*");
        w2 = new Text("*");
        w3 = new Text("*");
    }
    public Trigram(Text word1, Text word2, Text word3){
        w1 = new Text(word1);
        w2 = new Text(word2);
        w3 = new Text(word3);
    }
    public Trigram(Trigram trigram){
        w1 = new Text(trigram.w1);
        w2 = new Text(trigram.w2);
        w3 = new Text(trigram.w3);
    }
    public Trigram(String[] words){
        w1 = new Text(words[0]);
        w2 = new Text(words[1]);
        w3 = new Text(words[2]);
    }
    public Trigram(String word1, String word2, String word3){
        w1 = new Text(word1);
        w2 = new Text(word2);
        w3 = new Text(word3);
    }

    @Override
    public int compareTo(Trigram other) {
        int w1_compare = this.getW1().compareTo(other.getW1());
        if (w1_compare == 0){
            int w2_compare = this.getW2().compareTo(other.getW2());
            if (w2_compare == 0){
                return this.getW3().compareTo(other.getW3()); // TODO might need to delete this part
            }
            else return w2_compare;
        }
        else return w1_compare;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.w1.write(dataOutput);
        this.w2.write(dataOutput);
        this.w3.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.w1.readFields(dataInput);
        this.w2.readFields(dataInput);
        this.w3.readFields(dataInput);
    }

    public String getW1() {
        return this.w1.toString();
    }

    public String getW2() {
        return this.w2.toString();
    }

    public String getW3() {
        return this.w3.toString();
    }
    @Override
    public String toString() {
        return this.getW1() + " " + this.getW2() + " " + this.getW3();
    }
}
