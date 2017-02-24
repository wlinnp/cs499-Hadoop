package edu.cpp.cs499.A3.MovieMapping;

import edu.cpp.cs499.Misc;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author waiphyo
 *         2/24/17.
 */
public class MovieMapperClass2 extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split(Misc.COMMA);
        if (values.length >= Misc.MOVIE_RATING_COL) {
            StringBuilder movieTitle = new StringBuilder();
            for (int i = 2; i < values.length - 1; i++) {
                movieTitle.append(values[i]).append(Misc.COMMA);
            }
            movieTitle.append(values[values.length - 1]);
            context.write(new Text(values[0]), new Text(movieTitle.toString()));
        } else {
            System.out.println("MovieMapperClass2 Corrupted ==> " + value.toString());
        }
    }
}
