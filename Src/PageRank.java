import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRank {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: PageRank <input path> <output path>");
            System.exit(-1);
        }

        String input = args[0];
        String output = args[1];
        for(int i = 0; i < 3; i++) {
            Job job = Job.getInstance();
            job.setJarByClass(PageRank.class);
            job.setJobName("Page Rank" + i + "");

            FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path(output));

            job.setMapperClass(PageRankMapper.class);
            job.setReducerClass(PageRankReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setNumReduceTasks(1);

            output = output + i + "";
            job.waitForCompletion(true);s
            input = output + "/" + "part-r-00000";
          }
    context.write(NullWritable.get(), new Text(print + " " + rank_value + ""));
    }
}
