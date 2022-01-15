package SecondStep;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;

public class SecondStepValue implements WritableComparable<SecondStepValue> {

    private final LongWritable count;

    public SecondStepValue(){
        this.count = new LongWritable(-1);
    }
    public SecondStepValue(Long count){
        this.count = new LongWritable(count);
    }

    @Override
    public int compareTo(SecondStepValue other) {
        return this.count.compareTo(other.getCount());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.count.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.count.readFields(dataInput);
    }

    public LongWritable getCount() {
        return count;
    }

    public String toString(){
        return this.count.toString();
    }
}