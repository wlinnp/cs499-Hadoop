package edu.cpp.cs499.A3.UserCount;

import edu.cpp.cs499.Misc;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author waiphyo
 *         2/23/17.
 */
public class UserCountMapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {

    /**
     * Correct Format
     * 8,1744889,1.0
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] input = value.toString().split(Misc.COMMA);
        if (input.length == Misc.MOVIE_RATING_COL && Misc.isPositiveInteger(input[1])) {
            context.write(new Text(input[1]), Misc.INT_WRITABLE_ONE);
        }
    }
}
