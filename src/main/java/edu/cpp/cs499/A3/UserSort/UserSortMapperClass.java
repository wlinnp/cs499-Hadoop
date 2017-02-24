package edu.cpp.cs499.A3.UserSort;

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
public class UserSortMapperClass extends Mapper<LongWritable, Text, IntWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] input = value.toString().split(Misc.TAB);
        if (input.length == Misc.PROCESSED_LIST_COL) {
            context.write(new IntWritable(Integer.parseInt(input[1])), new Text(input[0]));
        }
    }
}
