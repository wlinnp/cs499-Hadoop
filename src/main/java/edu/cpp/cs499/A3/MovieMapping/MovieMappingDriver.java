package edu.cpp.cs499.A3.MovieMapping;

import edu.cpp.cs499.A3.MovieRating.MovieRatingMapperClass;
import edu.cpp.cs499.A3.MovieRating.MovieRatingReducerClass;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author waiphyo
 *         2/23/17.
 */
public class MovieMappingDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MovieMappingDriver(), args);
        System.exit(exitCode);
    }
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.printf("Usage: %s needs two arguments, input and output files\n", getClass().getSimpleName());
            return -1;
        }

        Job job = new Job();
        job.setJarByClass(MovieMappingDriver.class);
        job.setJobName("Movie-Counter");

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MovieMapperClass1.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MovieMapperClass2.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setReducerClass(MovieMappingReducerClass.class);

        int returnValue = job.waitForCompletion(true) ? 0:1;

        if (job.isSuccessful()) {
            System.out.println("Job was successful");
        } else if(!job.isSuccessful()) {
            System.out.println("Job was not successful");
        }

        return returnValue;
    }
}
/*
/media/waiphyo/General/cpp/cs499/HadoopPractice/RatedMovies
/media/waiphyo/General/cpp/cs499/HadoopPractice/movie_titles.txt
/media/waiphyo/General/cpp/cs499/HadoopPractice/MovieMapping
 */