package comp9313.proj1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class TFIDFReducer extends Reducer<TermDocIDPair, IntWritable, Text, DoubleWritable> {
	// (term, DocID), [count1, count2, ...] ==> (term, docID), sum
	
	// important variables:
	private DoubleWritable tfidf = new DoubleWritable();
	private int tf = 0;
	private int df = 0;
	private Text tdpair_txt = new Text();
	
	/* *
	 * Q: Could (term, *), (term, 1), ..., (term, #docID) come into one Reducer at once?
	 * A: No. Inner machanism of Hadoop will prevent such thing.
	 * */
	public void reduce(TermDocIDPair tdpair, Iterable<IntWritable> counts, Context context
						) throws IOException, InterruptedException {
		
		// get number of times the (term,doc) pair occurs:
		int sum = 0;
		for (IntWritable count: counts) {
			sum += count.get();
		}
		
		int doc_id = tdpair.getDocIDInt();
		if (doc_id == Integer.MIN_VALUE) {
			// tdpair is a special key, sum should be df
			df = sum;
					
		} else {
			// get total number of docs:
			Configuration config = context.getConfiguration();
			int no_doc = Integer.parseInt(config.get("no_doc"));
			// sum is tf:
			tf = sum;
			// calculate weight:
			tfidf.set(((double)tf)*Math.log10(((double)no_doc)/((double)df)));
			tdpair_txt.set(tdpair.getTermString() + "\t" 
										+ String.valueOf(tdpair.getDocIDInt()));
			context.write(tdpair_txt, tfidf);
		}
	}
}
