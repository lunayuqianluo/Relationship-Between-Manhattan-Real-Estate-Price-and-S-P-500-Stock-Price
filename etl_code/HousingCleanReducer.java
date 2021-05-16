// cc Clean Application to do data cleaning
// vv HousingCleanReducer
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HousingCleanReducer
  extends Reducer<Text, Text, Text, Text> {

  @Override
  public void reduce(Text key, Iterable<Text> values,
      Context context)
      throws IOException, InterruptedException {
	long price = 0;
	long area = 0;
	for (Text value : values) {
		String [] data = (value.toString()).split(",");
		price += Integer.parseInt(data[0]);
		area += Integer.parseInt(data[1]);
    	}
	long areaPrice = price / area;
	context.write(new Text(key), new Text(areaPrice + ""));
  }
}
