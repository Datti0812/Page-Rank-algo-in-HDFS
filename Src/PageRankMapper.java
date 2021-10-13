import java.io.IOException;
import javax.naming.Context;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper
    extends Mapper<LongWritable, Text, Text, Text> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String line = value.toString();
    String[] strArray = line.split(" ");
    String initial = strArray[0];
    int size = (strArray.length)-2;
    String copy = "";
    float rank_val = Float.parseFloat(strArray[strArray.length-1]);
    float val = rank_val/size;

    for (int i=1; i<size+1; i++) {
      String final_val = initial + "," + String.valueOf(val);
      copy = copy + strArray[i] + " ";
      System.out.println(strArray[i] + " " + final_val);
      context.write(new Text(strArray[i]), new Text (final_val));


}
    System.out.println(initial + " " + copy);
    context.write(new Text (initial), new Text(copy.trim()));

   }
}
