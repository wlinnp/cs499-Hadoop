package edu.cpp.cs499.A3.MovieSort;

import edu.cpp.cs499.Misc;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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
public class MovieSortDriverTop extends Configured implements Tool {

    private static int topLines = 10;

    public static int getTopLines() {
        return topLines;
    }

    public static void setTopLines(int topLines) {
        MovieSortDriverTop.topLines = topLines;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MovieSortDriverTop(), args);
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
        job.setJarByClass(MovieSortDriverTop.class);
        job.setJobName("Movie-Sorter");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setSortComparatorClass(DescendingFloatWritableComparable.class);
        job.setOutputKeyClass(FloatWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(MovieSortMapperClass.class);
        job.setReducerClass(MovieSortReducerClassTop.class);

        int returnValue = job.waitForCompletion(true) ? 0:1;

        if (job.isSuccessful()) {
            System.out.println("Job was successful");
        } else if(!job.isSuccessful()) {
            System.out.println("Job was not successful");
        }

        return returnValue;
    }
}
