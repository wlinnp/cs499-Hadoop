package edu.cpp.cs499;

import org.apache.hadoop.io.IntWritable;

/**
 * @author waiphyo
 *         2/23/17.
 */
public class Misc {
    public static final String COMMA = ",";
    public static final String SPACE = " ";
    public static final String TAB = "\t";
    public static final String PIPE = "|";
    public static final int MOVIE_RATING_COL = 3;
    public static final int PROCESSED_LIST_COL = 2;

    public static final IntWritable INT_WRITABLE_ONE = new IntWritable(1);
    public static boolean isPositiveInteger(final String input) {
        return input.matches("^\\d+$");
    }
    public static boolean isPositiveFloat(final String input) {
        return input.matches("(^\\d+$)|(^\\d+\\.\\d+$)");
    }
}
