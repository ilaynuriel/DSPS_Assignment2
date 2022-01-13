package FirstStep;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FirstStepValue implements WritableComparable<FirstStepValue> {

    private final LongWritable count; // TODO explain

    public FirstStepValue(){
        this.count = new LongWritable(-1);
    }
    public FirstStepValue(Long count){
        this.count = new LongWritable(count);
    }

    @Override
    public int compareTo(FirstStepValue other) {
        return this.count.compareTo(other.getCount());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }

    public LongWritable getCount() {
        return count;
    }
}
