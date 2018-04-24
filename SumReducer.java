/**
 * Heather Nolis
 * Data Science
 * SeattleU
 * HW1
 */

package solution;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * To define a reduce function for your MapReduce job, subclass
 * the Reducer class and override the reduce method.
 * The class definition requires four parameters: 
 * @param The data type of the input key - Text
 * @param The data type of the input value - IntWritable
 * @param The data type of the output key - Text
 * @param The data type of the output value - DoubleWritable
 */
public class SumReducer extends
    Reducer<Text, IntWritable, Text, IntWritable> {

  /**
   * The reduce method runs once for each key received from
   * the shuffle and sort phase of the MapReduce framework.
   * The method receives:
   * @param A key of type Text
   * @param A set of values of type IntWritable
   * @param A Context object
   */
  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
	  
	  int sum = 0;


    //for each value passed in by mapper
    for (IntWritable value : values) {
      
      //add them up
      sum += value.get();
    }
    
    //write it
    context.write(key, new IntWritable(sum));
 
 
  }
}