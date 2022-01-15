package SecondStep;

import FirstStep.Trigram;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SecondStepOutputKey implements WritableComparable<SecondStepOutputKey> {
        private final Trigram trigram;
        private final DoubleWritable probability;

    public SecondStepOutputKey() {
        this.trigram = new Trigram();
        this.probability = new DoubleWritable(0);
    }
    public SecondStepOutputKey(Trigram trig, double prob) {
        this.trigram = new Trigram(trig);
        this.probability = new DoubleWritable(prob);
    }

    @Override
    public int compareTo(SecondStepOutputKey other) {
        int w1_compare = this.getTrigram().getW1().compareTo(other.getTrigram().getW1());
        if (w1_compare == 0) {
            int w2_compare = this.getTrigram().getW2().compareTo(other.getTrigram().getW2());
            if (w2_compare == 0) {
                return this.probability.compareTo(other.getProbability());
            }
            else return w2_compare;
        }
        else return w1_compare;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }
    public DoubleWritable getProbability() { return this.probability; }

    public Trigram getTrigram() { return this.trigram; }

    public String toString(){
        return this.trigram.toString() + "\t" + this.probability.toString();
    }
}
