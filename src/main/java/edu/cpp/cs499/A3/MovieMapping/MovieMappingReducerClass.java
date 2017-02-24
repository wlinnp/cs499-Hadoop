package edu.cpp.cs499.A3.MovieMapping;

import edu.cpp.cs499.Misc;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author waiphyo
 *         2/24/17.
 */
public class MovieMappingReducerClass extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> allValues = new ArrayList<String>();
        for (Text each : values) {
            allValues.add(each.toString());
        }
        //System.out.println(allValues.toString());
        if (allValues.size() == 2) {
            int keyIndex = Misc.isPositiveFloat(allValues.get(0)) ? 0 : 1;
            context.write(new Text(allValues.get(keyIndex)), new Text(allValues.get(1 - keyIndex)));
        } else {
            System.out.println("Corrupted Data ==> " + allValues.toString());
        }
    }
}
