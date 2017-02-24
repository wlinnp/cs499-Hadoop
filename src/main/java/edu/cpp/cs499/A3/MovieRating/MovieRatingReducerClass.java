package edu.cpp.cs499.A3.MovieRating;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author waiphyo
 *         2/23/17.
 */
public class MovieRatingReducerClass extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        int totalVotes = 0;
        float totalVoteValue = 0;
        for (FloatWritable each : values) {
            totalVoteValue += each.get();
            totalVotes++;
        }
        context.write(key, new FloatWritable(totalVoteValue/ totalVotes));
    }
}
