/***
 *
 * @author Oscar Ashunrn
 * @version 1.0.0
 *
 */

package invertedindex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;

public class InvertedIndexReducer extends
        Reducer<Text, Text, Text, Text> {
    private IntWritable result = new IntWritable();

    /**
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //Holds Counts per document
        HashMap<String, Integer> counts_per_doc = new HashMap<>();
        //Holds word position and line number
        HashMap<String, List<String>> word_pos = new HashMap<>();
        int total_count =0;

        //For each value per key word
        for(Text val : values){

            //Split value into parts
            String[] parts = val.toString().split("\t");

            //count sum and get id
            int sum = Integer.parseInt(parts[0]);
            String docId = parts[1];
            total_count+= sum;

            //get line number and position in sentence
            String line_num = parts[2];
            String line_pos =parts[3];
            //if the  document id doesn't already exist, add it in
            if(!counts_per_doc.containsKey(docId)){
                counts_per_doc.put(docId,0);
                word_pos.put(docId,new ArrayList<>());
            }
            //add sum of the word with document id
            counts_per_doc.put(docId,counts_per_doc.get(docId)+sum);
            word_pos.get(docId).add(line_num + "\t" +line_pos ); // Storing document ID, line number, and position
            }
        //Write the word and its total sum to output
        context.write(key,new Text(Integer.toString(total_count)));

        //Write the document id, sum for that document and line and sentence position
        for(Map.Entry<String,Integer> entry: counts_per_doc.entrySet()){
            //get id and count
            String docID = entry.getKey();
            int count = entry.getValue();
            //get positions for that id
            List<String> positions = word_pos.get(docID);
            context.write(new Text("\t"+ docID), new Text(Integer.toString(count)));
            for (String pos : positions) {
                context.write(new Text("\t\t\t" + pos), new Text("")); // Indentation for clarity
            }
        }

        }

   }





