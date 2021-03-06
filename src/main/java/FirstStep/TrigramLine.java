package FirstStep;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class TrigramLine implements WritableComparable<TrigramLine> {

    private final Trigram trigram;
    private final IntWritable year;
    private final LongWritable occurrences;
    private final IntWritable pages;
    private final IntWritable books;

    public TrigramLine() {
//      The value is a tab separated string containing the following fields:
//      n-gram - The actual n-gram
//      year - The year for this aggregation
//      occurrences - The number of times this n-gram appeared in this year
//      pages - The number of pages this n-gram appeared on in this year
//      books - The number of books this n-gram appeared in during this year
//      The n-gram field is a space separated representation of the tuple.
//      Example: analysis is often\t1991\t1\t1\t1
        trigram = new Trigram();
        year = new IntWritable(0);
        occurrences = new LongWritable(0);
        pages = new IntWritable(0);
        books = new IntWritable(0);
    }

    public TrigramLine(Text lineText){
        String[] data = lineText.toString().split("\t");
        String[] gram = data[0].split(" ");
        trigram = new Trigram(gram);
        System.out.print("Data = " + Arrays.toString(data) + ", Trigram = " + Arrays.toString(gram));
        year = new IntWritable(Integer.parseInt(data[1]));
        occurrences = new LongWritable(Integer.parseInt(data[2]));
        pages = new IntWritable(Integer.parseInt(data[3]));
        books = new IntWritable(Integer.parseInt(data[4]));
    }

    public TrigramLine(Trigram other, IntWritable occur){
        trigram = new Trigram(other);
        year = new IntWritable(0);
        occurrences = new LongWritable(occur.get());
        pages = new IntWritable(0);
        books = new IntWritable(0);
    }

    @Override
    public int compareTo(TrigramLine other) {
        // Sort according to lexicographic ascending order:
        return trigram.compareTo(other.trigram);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.trigram.write(dataOutput);
        this.year.write(dataOutput);
        this.occurrences.write(dataOutput);
        this.pages.write(dataOutput);
        this.books.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        trigram.readFields(dataInput);
        year.readFields(dataInput);
        occurrences.readFields(dataInput);
        pages.readFields(dataInput);
        books.readFields(dataInput);
    }
    public Trigram getTrigram() {
        return this.trigram;
    }

    public IntWritable getYear() { return this.year; }

    public LongWritable getOccurrences() { return this.occurrences; }

    public IntWritable getPages() { return this.pages; }

    public IntWritable getBooks() { return this.books; }

    @Override
    public String toString() {
        return  this.trigram.toString() + "\t" +
                this.getYear().toString() + "\t" +
                this.getOccurrences().toString() + "\t" +
                this.getPages().toString() + "\t" +
                this.getBooks().toString();
    }

    public Text toText() {
        return new Text(this.toString());
    }

}
