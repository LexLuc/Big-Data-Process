package comp9313.proj1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TermDocIDPair implements WritableComparable<TermDocIDPair> {

	private Text term; 
	private IntWritable docID;

	public TermDocIDPair() {
		// Empty Term-DocID Pair:
		set(new Text(), new IntWritable());
	}
	
	public TermDocIDPair(String term, Integer docID) {
		// Term-DocID Pair by Java built-in:
		set(new Text(term), new IntWritable(docID.intValue()));
	}

	public TermDocIDPair(Text term, IntWritable docID) {
		// Term-DocID Pair by Hadoop-wrapped class:
		set(term, docID);
	}

	public void set(Text left, IntWritable right) {
		this.term = left;
		this.docID = right;
	}
	
	public void set(String left, Integer right) {
		this.term = new Text(left);
		this.docID = new IntWritable(right);
	}

	public Text getTerm() {
		return this.term;
	}
	
	public String getTermString() {
		return this.term.toString();
	}

	public IntWritable getDocID() {
		return this.docID;
	}
	
	public int getDocIDInt() {
		return this.docID.get();
	}

	public void readFields(DataInput in) throws IOException {
		this.term.readFields(in);
		this.docID.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		this.term.write(out);
		this.docID.write(out);
	}

	@Override
	public String toString() {
		return this.term.toString() + ", " + this.docID.toString();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof TermDocIDPair) {
			TermDocIDPair tdp = (TermDocIDPair) o;
			return this.term.equals(tdp.term) 
				&& this.docID.equals(tdp.docID);
		}
		return false;
	}

	@Override
	// to ensure that all the (key,value) pairs 
	// with same term key are sent to the same reducer:
	public int hashCode() {
		return this.term.hashCode();
	}

	@Override
	// firstly, compare the terms in two TermDocIDPairs;
	// if terms are same, then compare the docID 
	// to ensure secondary sort works properly:
	public int compareTo(TermDocIDPair it) {
		int cmp = this.term.compareTo(it.term);
		if(cmp != 0){
			return cmp;
		}
		return this.docID.compareTo(it.docID);
	}	

}
