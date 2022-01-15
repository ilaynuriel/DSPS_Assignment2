package SecondStep;

import FirstStep.Trigram;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SecondStepOutputValue implements WritableComparable<SecondStepOutputValue> {
    private final DoubleWritable probability;

    public SecondStepOutputValue() {
        this.probability = new DoubleWritable(0);
    }
    public SecondStepOutputValue(double prob) {
        this.probability = new DoubleWritable(prob);
    }

    @Override
    public int compareTo(SecondStepOutputValue other) {
        return this.getProbability().compareTo(other.getProbability());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.probability.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.probability.readFields(dataInput);
    }
    public DoubleWritable getProbability() { return this.probability; }

    public String toString(){
        return this.probability.toString();
    }
}
