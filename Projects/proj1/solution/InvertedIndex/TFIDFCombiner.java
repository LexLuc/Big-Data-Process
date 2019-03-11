package comp9313.proj1;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class TFIDFCombiner extends Reducer<TermDocIDPair, IntWritable, TermDocIDPair, IntWritable> {
	// (term, docID), [1, 1, ...] ==> (term, docID), count
	
	private IntWritable partial_sum = new IntWritable();
	
	public void reduce(TermDocIDPair tdpair, Iterable<IntWritable> ones, Context context
						) throws IOException, InterruptedException {
		// aggregate "1"s to improve efficiency:
		int sum = 0;
		for (IntWritable one: ones) {
			sum += one.get();
		}
		partial_sum.set(sum);
		context.write(tdpair, partial_sum);
	}
}
