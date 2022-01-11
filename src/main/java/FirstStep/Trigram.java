package FirstStep;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TrigramLine implements WritableComparable<TrigramLine> {
//
//    private final Text w1;
//    private final Text w2;
//    private final Text w3;
    private final Trigram trigram;
    private final IntWritable year;
    private final LongWritable occurrences;
    private final IntWritable pages;
    private final IntWritable books;

    public Trigram() {
/*    The value is a tab separated string containing the following fields:
      n-gram - The actual n-gram
      year - The year for this aggregation
      occurrences - The number of times this n-gram appeared in this year
      pages - The number of pages this n-gram appeared on in this year
      books - The number of books this n-gram appeared in during this year
      The n-gram field is a space separated representation of the tuple.
      Example: analysis is often\t1991\t1\t1\t1                            */
        w1 = new Text("*");
        w2 = new Text("*");
        w3 = new Text("*");
        year = new IntWritable(-1);
        occurrences = new LongWritable(-1);
        pages = new IntWritable(-1);
        books = new IntWritable(-1);
    }

    public Trigram(Text lineText){
    //  lineText should be a tab separated string containing the following fields:
    //  n-gram - The actual n-gram
    //  year - The year for this aggregation
    //  occurrences - The number of times this n-gram appeared in this year
    //  pages - The number of pages this n-gram appeared on in this year
    //  books - The number of books this n-gram appeared in during this year
    //  The n-gram field is a space separated representation of the tuple.
        String[] data = lineText.toString().split("\t");
        String[] gram = data[0].split(" ");
        w1 = new Text(gram[0]);
        w2 = new Text(gram[1]);
        w3 = new Text(gram[2]);
        year = new IntWritable(Integer.parseInt(data[1]));
        occurrences = new LongWritable(Integer.parseInt(data[2]));
        pages = new IntWritable(Integer.parseInt(data[3]));
        books = new IntWritable(Integer.parseInt(data[4]));
    }

    public Trigram(Text word1, Text word2, Text word3, IntWritable occur){
        w1 = new Text(word1);
        w2 = new Text(word2);
        w3 = new Text(word3);
        year = new IntWritable(-1);
        occurrences = new LongWritable(occur.get());
        pages = new IntWritable(-1);
        books = new IntWritable(-1);
    }

    @Override
    public int compareTo(Trigram other) {
        // Sort according to lexicographic ascending order:
        int w1_compare = this.getW1().compareTo(other.getW1());
        if (w1_compare == 0){
            int w2_compare = this.getW2().compareTo(other.getW2());
            if (w2_compare == 0){
                return this.getW3().compareTo(other.getW3());
            }
            else return w2_compare;
        }
        else return w1_compare;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
//        w1.write(dataOutput);
//        w2.write(dataOutput);
//        w3.write(dataOutput);
//        decade.write(dataOutput);
//        type.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {  // TODO: how readFields works?
        w1.readFields(dataInput);
        w2.readFields(dataInput);
        w3.readFields(dataInput);
        year.readFields(dataInput);
        occurrences.readFields(dataInput);
        pages.readFields(dataInput);
        books.readFields(dataInput);
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

    public int getYear() { return this.year.get(); }

    public String getOccurrences() { return this.occurrences.toString(); }

    public String getPages() {
        return this.pages.toString();
    }

    public String getBooks() {
        return this.books.toString();
    }

    @Override
    public String toString() {
        return  this.getW1() + " " +
                this.getW2() + " " +
                this.getW3()  + "\t" +
                this.getYear() + "\t" +
                this.getOccurrences() + "\t" +
                this.getPages() + "\t" +
                this.getBooks();
    }

    public Text toText() {
        return new Text(this.toString());
    }

}
