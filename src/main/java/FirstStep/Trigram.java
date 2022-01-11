package FirstStep;

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

    }
    public Trigram() {

    }

    @Override
    public int compareTo(Trigram o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }
}
