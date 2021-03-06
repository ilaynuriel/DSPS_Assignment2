package FirstStep;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FirstStepValue implements WritableComparable<FirstStepValue> {

    private final LongWritable count; // TODO explain
    private final Trigram trigram;

    public FirstStepValue(){
        this.trigram = new Trigram();
        this.count = new LongWritable(-1);
    }
    public FirstStepValue(Trigram trigram, Long count){
        this.trigram = new Trigram(trigram);
        this.count = new LongWritable(count);
    }

    @Override
    public int compareTo(FirstStepValue other) {
        return this.count.compareTo(other.getCount());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.trigram.write(dataOutput);
        this.count.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.trigram.readFields(dataInput);
        this.count.readFields(dataInput);
    }

    public LongWritable getCount() {
        return count;
    }

    public Trigram getTrigram() {
        return trigram;
    }

    public String toString(){
        return this.trigram.toString() + "\t" + this.count.toString();
    }
}
