// cc Clean Application to do data cleaning
// vv StockCleanReducer
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StockCleanReducer
  extends Reducer<Text, Text, Text, Text> {
  
  public void reduce(Text key, Iterable<Text> values,
      Context context)
      throws IOException, InterruptedException {
	double price = 0;
	int count = 0;
	for (Text value : values) {
		price += Double.parseDouble(value.toString());
		count += 1;
    	}
	long avgPrice = Math.round(price / count);
	context.write(new Text(key), new Text(avgPrice + ""));
  }
}
