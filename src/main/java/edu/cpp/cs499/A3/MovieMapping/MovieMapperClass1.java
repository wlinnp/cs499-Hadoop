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
public class MovieMapperClass1 extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split(Misc.TAB);
        if (values.length == Misc.PROCESSED_LIST_COL) {
            context.write(new Text(values[0]), new Text(values[1]));
        } else {
            System.out.println("MovieMapperClass1 Corrupted ==> " + value.toString());
        }
    }
}
