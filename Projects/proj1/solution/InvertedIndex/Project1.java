package comp9313.proj1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Project1 {
	public static void main(String[] args) throws Exception {
		// Configurations:
		Configuration config = new Configuration();
		config.set("fs.default.name", "hdfs://localhost:9000");
		// get number of docs
		URI input_uri = URI.create(args[0]);
		FileSystem fs =  FileSystem.get(config);
		RemoteIterator<LocatedFileStatus> fileitr = fs.listFiles(new Path(input_uri), true);
		FSDataInputStream in = fs.open(fileitr.next().getPath()); // assume there's only one file in input DIR
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		int lines = 0;
		while (reader.readLine()!=null) {
			lines ++;
		} reader.close();
		// set number of docs
		config.set("no_doc", String.valueOf(lines));
		//set output delimiter to comma
		config.set("mapred.textoutputformat.separator", ",");
		Job job = Job.getInstance(config, "TF-IDF");
		job.setJarByClass(Project1.class);
		
		// set Mapper:
		job.setMapperClass(TFIDFMapper.class);
		job.setMapOutputKeyClass(TermDocIDPair.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		// set Combiner:
		job.setCombinerClass(TFIDFCombiner.class);
		
		// set Reducer:
		job.setReducerClass(TFIDFReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setNumReduceTasks(Integer.parseInt(args[2]));
		
		// set output path:
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    // wait until MR task finished and return status code to OS:
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}
