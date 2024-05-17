/***
 *
 * @author Oscar Ashunrn
 * @version 1.0.0
 *
 */
package invertedindex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text  > {

    private final IntWritable one = new IntWritable(1);
    //val that holds document, line number and position
    private Text doc_info = new Text();
    private final IntWritable line_num = new IntWritable(1);
    //current document id
    private String id;

    //Current word
    private Text word = new Text();

    //HashSet to store stopwords
    private Set<String> stopwords = new HashSet<>();

    /**
     *
     * Run stop words on construction
     */
    public InvertedIndexMapper(){
            readStopWords();
        }

    /**
     * Reads in stop words so we can skip over them
     */
    public void readStopWords(){
        String filepath  = "/home/oa57/Desktop/COMPX553/assignment-six-hadoop-inverted-2024/src/invertedindex/stop_words.txt";
            try (BufferedReader br = new BufferedReader(new FileReader(filepath))) {
                String line;
                // Read the file line by line
                while ((line = br.readLine()) != null) {
                    // Skip lines that start with #
                    if (line.startsWith("#")) {
                        continue;
                    }
                    // Split the line by spaces
                    String[] words = line.split("\\s+");
                    // Check if the line is not empty and has words
                    if (words.length > 0) {
                        // Add the first word to the list
                        stopwords.add(words[0]);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    /**
     *
     * @param key
     * @param value Document to read
     * @param context context
     * @throws IOException
     * @throws InterruptedException
     */
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            //get values
            String text = value.toString();


            //split the text in to lines
            String[] lines = text.split("\n");

            for(String line: lines){
                //check if the line contains the document id
                if (line.contains("Document") && line.contains("id")) {
                    id = line.replaceAll("[^0-9]", "");
                }
                //split line into words
                String[] words = line.split(" ");
                for (int i = 0; i < words.length; i++) {
                    if(stopwords.contains(words[i])){
                        context.getCounter(Counters.STOP_WORDS).increment(1);
                        continue;
                    }
                    //get the current word
                    word.set(words[i]);
                    context.getCounter(Counters.TOTAL_CHARACTERS).increment(word.getLength());
                    context.getCounter(Counters.WORD_COUNT).increment(1);
                    if (words[i].startsWith("A") || words[i].startsWith("a")) {
                        context.getCounter(Counters.A_WORDS).increment(1);
                    } else if (words[i].startsWith("Z") || words[i].startsWith("z")) {
                        context.getCounter(Counters.Z_WORDS).increment(1);
                    }
                    //set token to contain count,doc id, line number and sentence position
                    doc_info.set("1"+"\t"+ id + "\t" + line_num + "\t" + i);
                    //write the word as the key and the
                    context.write(word, doc_info);
                }
                //reset line number
                line_num.set(line_num.get() + 1);
            }
            line_num.set(1);
        }

}

