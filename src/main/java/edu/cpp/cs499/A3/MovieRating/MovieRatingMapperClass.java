package edu.cpp.cs499.A3.MovieRating;

import edu.cpp.cs499.Misc;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author waiphyo
 *         2/23/17.
 */
public class MovieRatingMapperClass extends Mapper<LongWritable, Text, Text, FloatWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] input = value.toString().split(Misc.COMMA);
        if (input.length == Misc.MOVIE_RATING_COL && Misc.isPositiveInteger(input[1])) {
            context.write(new Text(input[0]), new FloatWritable(Float.parseFloat(input[2])));
        }
    }
}
