package edu.cpp.cs499.A3.UserSort;

import edu.cpp.cs499.Misc;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author waiphyo
 *         2/23/17.
 */
public class UserSortDriverTop extends Configured implements Tool {

    private static int topLines = 10;

    public static int getTopLines() {
        return topLines;
    }

    public static void setTopLines(int topLines) {
        UserSortDriverTop.topLines = topLines;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new UserSortDriverTop(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.printf("Usage: %s needs two arguments, input and output files\n", getClass().getSimpleName());
            return -1;
        }

        if (args.length >= 3 && Misc.isPositiveInteger(args[2])) {
            setTopLines(Integer.parseInt(args[2]));
        }
        Job job = new Job();
        job.setJarByClass(UserSortDriverTop.class);
        job.setJobName("User-Sorter");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setSortComparatorClass(DescendingIntWritableComparable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(UserSortMapperClass.class);
        job.setReducerClass(UserSortReducerClassTop.class);

        int returnValue = job.waitForCompletion(true) ? 0:1;

        if (job.isSuccessful()) {
            System.out.println("Job was successful");
        } else if(!job.isSuccessful()) {
            System.out.println("Job was not successful");
        }

        return returnValue;
    }
}
