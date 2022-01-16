package SecondStep;

import FirstStep.Trigram;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SecondStepKey implements WritableComparable<SecondStepKey> {
    // TODO: add explanations about the values of the key:
    private final Trigram trigram;
    private final Text word1;
    private final Text word2;
    private final Text tag;

    public SecondStepKey() {
        this.tag = new Text("x");
        this.word1 = new Text("!");
        this.word2 = new Text("!");
        this.trigram = new Trigram();
    }

    public SecondStepKey(Trigram trigram, Character tag) {
        this.tag = new Text(String.valueOf(tag));
        this.word1 = new Text("!");
        this.word2 = new Text("!");
        this.trigram = new Trigram(trigram);
    }

    public SecondStepKey(Trigram trigram, String word1, String word2, Character tag) {
        this.tag = new Text(String.valueOf(tag));
        this.word1 = new Text(word1);
        this.word2 = new Text(word2);
        this.trigram = new Trigram(trigram);
    }

    @Override
    public int hashCode() {
        return trigram.getW1().hashCode();
    }

    @Override
    public int compareTo(SecondStepKey other) {
        int compareTrigram = this.trigram.compareTo(other.trigram);
        if (compareTrigram == 0)
            return -1 * this.tag.compareTo(other.tag); // we want i tagged before c tagged
        else
            return compareTrigram;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        trigram.write(dataOutput);
        word1.write(dataOutput);
        word2.write(dataOutput);
        tag.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        trigram.readFields(dataInput);
        word1.readFields(dataInput);
        word2.readFields(dataInput);
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

    public Trigram getTrigram() { return trigram; }

    public String toString(){
        return this.trigram.toString() + "\t" +
                this.word1.toString() + "\t" +
                this.word2.toString() +"\t" +
                this.tag.toString();
    }
}
