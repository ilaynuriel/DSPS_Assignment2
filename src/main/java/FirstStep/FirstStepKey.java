package FirstStep;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FirstStepKey implements WritableComparable<FirstStepKey> {
    // TODO: add explanations about the values of the key:
    private final Text word1;
    private final Text word2;
    private final Text word3;
    private final Text tag;

    public FirstStepKey(){
        this.tag = new Text();
        this.word1 = new Text();
        this.word2 = new Text();
        this.word3 = new Text();
    }
    public FirstStepKey(Text word, Text tag){
        this.tag = tag;
        this.word1 = word;
        this.word2 = word;
        this.word3 = word;
    }
    public FirstStepKey(String word1, Character tag){
        this.tag = new Text(String.valueOf(tag));
        this.word1 = new Text(word1);
        this.word2 = new Text("!");
        this.word3 = new Text("!");
    }
    public FirstStepKey(String word1, String word2, String word3, Character tag){
        this.tag = new Text(String.valueOf(tag));
        this.word1 = new Text(word1);
        this.word2 = new Text(word2);
        this.word3 = new Text(word3);
    }

    @Override
    public int hashCode() {
        return word1.toString().hashCode();
    }

    @Override
    public int compareTo(FirstStepKey other) { // TODO understand if this correct
        int compareWord1 = this.word1.compareTo(other.word1);
        if (compareWord1 == 0){
            int compareWord2 = this.word2.compareTo(other.word2);
            if (compareWord2 == 0) {
                return this.tag.compareTo(other.tag);
            }
            else
                return compareWord2;
        }
        else
            return compareWord1;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        word1.write(dataOutput);
        word2.write(dataOutput);
        word3.write(dataOutput);
        tag.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        word1.readFields(dataInput);
        word2.readFields(dataInput);
        word3.readFields(dataInput);
        tag.readFields(dataInput);
    }

    public Text getTag() {
        return tag;
    }

    public Text getWord1() {
        return word1;
    }

    public Text getWord2() {
        return word2;
    }

    public Text getWord3() {
        return word3;
    }

    public String toString(){
        return this.word1.toString() + "\t" +
                this.word2.toString() + "\t" +
                this.word3.toString() +"\t" +
                this.tag.toString();
    }
}
