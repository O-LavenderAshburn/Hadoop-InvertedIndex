package invertedindex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class InvertedIndexPartitioner extends Partitioner<Text, Text> {
    private int max = 31;
    @Override
    public int getPartition(Text key, Text value, int numPartitions) {
        int wordLength = key.getLength();
        // Calculate the range size for each partition
        int rangeSize = (int) Math.ceil((double) max / numPartitions);
        // Determine the partition based on word length
        int partition = (wordLength - 1) / rangeSize;
        // Ensure partition index is within bounds [0, numPartitions-1]
        return Math.min(partition, numPartitions - 1);

    }
}

