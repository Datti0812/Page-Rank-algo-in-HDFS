import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer
    extends Reducer<Text, Text, NullWritable, Text> {

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    float rank_value = 0.0f;
    String print = "";
    for (Text value: values) {
      String out = value.toString();
      if(!out.contains(",")){
        print = key + " " + out;
        continue;
      }
      else if(out.contains(",")){
        String[] temp = out.split(",");
        rank_value += Float.parseFloat(temp[1]);
    }
}

    context.write(NullWritable.get(), new Text(print + " " + rank_value + ""));
  }
}

