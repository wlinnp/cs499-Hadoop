package edu.cpp.cs499.A3.UserSort;

import edu.cpp.cs499.Misc;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author waiphyo
 *         2/23/17.
 */
public class UserSortReducerClass extends Reducer<IntWritable, Text,  IntWritable, Text> {
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder result = new StringBuilder();
        for (Text each : values) {
            result.append(each.toString()).append(Misc.COMMA);
        }
        context.write(key, new Text(result.toString()));
    }
}
