package invertedindex;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class InvertedIndexComparator extends WritableComparator {
    // Constructor
    protected InvertedIndexComparator(){
        // Call the superclass constructor with the Text class and true for creating instances
        super(Text.class, true);
    }

    // Overriding the compare method to compare two Text objects
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        // Cast the arguments to Text
        Text wp1 = (Text) a;
        Text wp2 = (Text) b;

        int comp = Integer.compare(wp1.getLength(), wp2.getLength());
        if(comp == 0){
            return a.compareTo(b);
        }else
            return comp;
    }
}