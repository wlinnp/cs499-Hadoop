package edu.cpp.cs499.A3;

import edu.cpp.cs499.A3.MovieMapping.MovieMapperClass1;
import edu.cpp.cs499.A3.MovieMapping.MovieMapperClass2;
import edu.cpp.cs499.A3.MovieMapping.MovieMappingDriver;
import edu.cpp.cs499.A3.MovieMapping.MovieMappingReducerClass;
import edu.cpp.cs499.A3.MovieRating.MovieRatingDriver;
import edu.cpp.cs499.A3.MovieRating.MovieRatingMapperClass;
import edu.cpp.cs499.A3.MovieRating.MovieRatingReducerClass;
import edu.cpp.cs499.A3.MovieSort.DescendingFloatWritableComparable;
import edu.cpp.cs499.A3.MovieSort.MovieSortMapperClass;
import edu.cpp.cs499.A3.MovieSort.MovieSortReducerClassTop;
import edu.cpp.cs499.Misc;
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
 *         2/27/17.
 */
public class MovieDriverTop extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MovieDriverTop(), args);
        System.exit(exitCode);
    }
    public int run(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.printf("Usage: %s needs two arguments, input and output files\n", getClass().getSimpleName());
            return -1;
        }
        String ratingJobName = "MovieRatings";
        String mappingJobName = "MovieMappings";
        String sortingJobName = "MovieSortingTop";

        Job ratingJob = new Job();
        ratingJob.setJarByClass(MovieDriverTop.class);
        ratingJob.setJobName("Movie Average Rating Job");
        FileInputFormat.addInputPath(ratingJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(ratingJob, new Path(args[2]
        + Misc.PATH_SEPARATOR
        + ratingJobName));

        ratingJob.setOutputKeyClass(Text.class);
        ratingJob.setOutputValueClass(FloatWritable.class);
        ratingJob.setOutputFormatClass(TextOutputFormat.class);

        ratingJob.setMapperClass(MovieRatingMapperClass.class);
        ratingJob.setReducerClass(MovieRatingReducerClass.class);

        int returnValue = ratingJob.waitForCompletion(true) ? 0:1;

        if (!ratingJob.isSuccessful()) {
            System.out.println(ratingJob.getJobName() + " failed");
            return returnValue;
        }

        Job mappingJob = new Job();
        mappingJob.setJarByClass(MovieDriverTop.class);
        mappingJob.setJobName("Movie ID to name mapping job");

        MultipleInputs.addInputPath(mappingJob, new Path(args[2]
        + Misc.PATH_SEPARATOR
        + ratingJobName
        + Misc.PATH_SEPARATOR
        + Misc.STANDARD_HADOOP_OUTPUT), TextInputFormat.class, MovieMapperClass1.class);
        MultipleInputs.addInputPath(mappingJob, new Path(args[1]), TextInputFormat.class, MovieMapperClass2.class);
        FileOutputFormat.setOutputPath(mappingJob, new Path(args[2]
        + Misc.PATH_SEPARATOR
        + mappingJobName));

        mappingJob.setOutputKeyClass(Text.class);
        mappingJob.setOutputValueClass(Text.class);
        mappingJob.setOutputFormatClass(TextOutputFormat.class);

        mappingJob.setReducerClass(MovieMappingReducerClass.class);

        returnValue = mappingJob.waitForCompletion(true) ? 0:1;

        if(!mappingJob.isSuccessful()) {
            System.out.println(mappingJob.getJobName() + " failed");
            return returnValue;
        }

        if (args.length >= 4 && Misc.isPositiveInteger(args[3])) {
            TopTen.getInstance().setCount(Integer.parseInt(args[3]));
        } else {
            TopTen.getInstance().setCount(10);
        }

        Job sortingJob = new Job();
        sortingJob.setJarByClass(MovieDriverTop.class);
        sortingJob.setJobName("Top Ten average rating movie");

        FileInputFormat.addInputPath(sortingJob, new Path(args[2]
        + Misc.PATH_SEPARATOR
        + mappingJobName
        + Misc.PATH_SEPARATOR
        + Misc.STANDARD_HADOOP_OUTPUT));
        FileOutputFormat.setOutputPath(sortingJob, new Path(args[2]
        + Misc.PATH_SEPARATOR
        + sortingJobName));

        sortingJob.setSortComparatorClass(DescendingFloatWritableComparable.class);
        sortingJob.setOutputKeyClass(FloatWritable.class);
        sortingJob.setOutputValueClass(Text.class);
        sortingJob.setOutputFormatClass(TextOutputFormat.class);
        sortingJob.setMapperClass(MovieSortMapperClass.class);
        sortingJob.setReducerClass(MovieSortReducerClassTop.class);


        returnValue = sortingJob.waitForCompletion(true) ? 0:1;

        if (sortingJob.isSuccessful()) {
            System.out.println(sortingJob.getJobName() + " succeeded");
        } else if(!sortingJob.isSuccessful()) {
            System.out.println(sortingJob.getJobName() + " failed");
        }

        return returnValue;
    }
}
