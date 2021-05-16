// cc CountRecs Application to count the number of records in a dataset
// vv CountRecs
import java.io.IOException;
import java.util.HashMap;
import java.util.Map; 

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountRecsMapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {
  
  @Override
  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {
   
    	context.write(new Text("LineCount"), new IntWritable(1));

  }
}
