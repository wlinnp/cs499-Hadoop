package edu.cpp.cs499.A3.MovieSort;

import edu.cpp.cs499.Misc;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author waiphyo
 *         2/23/17.
 */
public class MovieSortReducerClassTop extends Reducer<FloatWritable, Text,  FloatWritable, Text> {
    private static int topLines = MovieSortDriverTop.getTopLines();
    @Override
    protected void reduce(FloatWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder result = new StringBuilder();
        for (Text each : values) {
            result.append(each.toString()).append(Misc.PIPE);
        }
            context.write(key, new Text(result.toString()));
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        this.setup(context);
        try {
            while(context.nextKey()) {
                if (topLines-- > 0) {
                    this.reduce(context.getCurrentKey(), context.getValues(), context);
                } else {
                    break;
                }
            }
        } finally {
            this.cleanup(context);
        }
    }
}
