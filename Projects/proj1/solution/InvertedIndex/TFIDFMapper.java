package comp9313.proj1;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.htrace.commons.logging.Log;
//import org.apache.htrace.commons.logging.LogFactory;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

public class TFIDFMapper extends Mapper<Object, Text, TermDocIDPair, IntWritable> {
	// key, Doc ==> (term, docID), one
	
	private TermDocIDPair tdpair = new TermDocIDPair();
	private final IntWritable one = new IntWritable(1);
	
	
	public void map(Object key, Text doc, Context context
					) throws IOException, InterruptedException {
		
		/*Log log = LogFactory.getLog(TFIDFMapper.class);*/
		
		// tokenize current doc:
		StringTokenizer itr = new StringTokenizer(doc.toString().toLowerCase());
		
		// build a set to prevent counting duplicated special key in one doc
		HashSet<String> set = new HashSet<String>();
		
		// read docID:
		assert itr.hasMoreTokens(): " No Document ID!";
		Integer doc_id = Integer.valueOf(itr.nextToken());
		
		while(itr.hasMoreTokens()) {
			String term = itr.nextToken();
			
			if (!set.contains(term)) {
				// the first time that the term appears in the current doc
				set.add(term);
				
				// emit special key (term, -inf):
				tdpair.set(term, Integer.MIN_VALUE); // ensure special key will reach reducer first after shuffle&sort session
				context.write(tdpair, one);
				
				/*log.info("log@MMMMMMMMMaper: ((((((" + term + ",*))))))");*/
			} 
			// emit normal key (term, doc_id):
			tdpair.set(term, doc_id);
			context.write(tdpair, one);
			
			/*log.info("log@MMMMMMMMMaper: ((((((" + term + "," + doc_id.toString() + "))))))");*/
		}
	}
}
