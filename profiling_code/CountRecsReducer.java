// cc CountRecs Application to count the number of records in a dataset
// vv CountRecs
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountRecsReducer
  extends Reducer<Text, IntWritable, Text, IntWritable> {
  
  @Override
  public void reduce(Text key, Iterable<IntWritable> values,
      Context context)
      throws IOException, InterruptedException {
    
    int count = 0;
    for (IntWritable value : values) {
      count = count + value.get();
    }
    
    context.write(new Text("Total number of records in the dataset:"), new IntWritable(count));
  }
}
