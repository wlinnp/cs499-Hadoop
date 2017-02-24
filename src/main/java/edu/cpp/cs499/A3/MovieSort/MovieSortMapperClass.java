package edu.cpp.cs499.A3.MovieSort;

import edu.cpp.cs499.Misc;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author waiphyo
 *         2/23/17.
 */
public class MovieSortMapperClass extends Mapper<LongWritable, Text, FloatWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split(Misc.TAB);
        if (values.length == Misc.PROCESSED_LIST_COL && Misc.isPositiveFloat(values[0])) {
            context.write(new FloatWritable(Float.parseFloat(values[0])), new Text(values[1]));
        } else {
            System.out.println("MovieSortMapperClass Corrupted ==> " + value.toString());
        }
    }
}
