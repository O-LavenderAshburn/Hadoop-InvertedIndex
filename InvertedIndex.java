/**
 *Inverted Index class to set up jobs
 * @author Oscar Ashburn
 * @version 1.0.0
 */
package invertedindex;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import wordcount.Map;
import wordcount.Reduce;
import wordcount.WordCount;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;


public class InvertedIndex extends Configured implements Tool{
		 public int run(String[] args) throws Exception {
			  if (args.length != 2) {
			   		System.err.printf("Usage: %s [generic options] <input> <output>\n",
			   		getClass().getSimpleName());
		 	         ToolRunner.printGenericCommandUsage(System.err);
		 	        return -1;
		 	 }

			Configuration conf = new Configuration();
			  //set delimiter
			  conf.set("textinputformat.record.delimiter","</Document>");
			  //Setup Job
			 Job job = Job.getInstance(conf, "inverted index");
			 job.setJarByClass(InvertedIndex.class);
			 job.setOutputKeyClass(Text.class);
			 job.setOutputValueClass(Text.class);

			 //Set classes  for __
			 //Mapper
			 job.setMapperClass(InvertedIndexMapper.class);
			 //Reducer
			 job.setReducerClass(InvertedIndexReducer.class);
			 //Comparator
			 job.setSortComparatorClass(InvertedIndexComparator.class);
			 //Combiner	//Yet to be implemented\\
			 // job.setCombinerClass(InvertedIndexCombiner.class);
			 //Partitioner
			 job.setPartitionerClass(InvertedIndexPartitioner.class);
			 //Set reduce tasks
			 job.setNumReduceTasks(4);
			 //set input and output
			 job.setInputFormatClass(TextInputFormat.class);
			 job.setOutputFormatClass(TextOutputFormat.class);
			 //Set input and output paths
			 FileInputFormat.setInputPaths(job, new Path(args[0]));
			 FileOutputFormat.setOutputPath(job, new Path(args[1]));


			 boolean success = job.waitForCompletion(true);
			 //Get custom counters if successful
			 if(success){
				 //Count of number of stop words
				 Counter counter = job.getCounters().findCounter(Counters.STOP_WORDS);
				 System.out.println("Stop words:	" + counter.getValue());
				 //Count of number of total characters
				 Counter counter2 = job.getCounters().findCounter(Counters.TOTAL_CHARACTERS);
				 System.out.println("Total characters:	" + (Long)counter2.getValue());
				 //Total word count
				 Counter counter3= job.getCounters().findCounter(Counters.WORD_COUNT);
				 System.out.println("Word Count:	" + (Long)counter3.getValue());
				 //Get counter of all words that start with A or a
				 Counter counter4 = job.getCounters().findCounter(Counters.A_WORDS);
				 System.out.println("A Words:	" + (Long)counter4.getValue());
				 //Get counter of all words that start with Z or z
				 Counter counter5 = job.getCounters().findCounter(Counters.Z_WORDS);
				 System.out.println("Z Words:	" + (Long)counter4.getValue());

			 }
			 //return success
			 return success ? 0 : 1;

		  }
		  
		  public static void main(String[] args) throws Exception {

			  String outputFile  = "/home/oa57/Desktop/COMPX553/assignment-six-hadoop-inverted-2024/output";
			  if (Files.isDirectory(Paths.get(outputFile))) {
				  FileUtils.forceDelete(new File(outputFile));
			  }
			  int exitCode = ToolRunner.run(new InvertedIndex(), args);
		      System.exit(exitCode);
		}
}


