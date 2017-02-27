package edu.cpp.cs499;

import org.apache.hadoop.io.IntWritable;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * @author waiphyo
 *         2/23/17.
 */
public class Misc {
    public static final String COMMA = ",";
    public static final String SPACE = " ";
    public static final String TAB = "\t";
    public static final String PIPE = "|";
    public static final String PATH_SEPARATOR = "/";

    public static final int MOVIE_RATING_COL = 3;
    public static final int PROCESSED_LIST_COL = 2;
    public static final String STANDARD_HADOOP_OUTPUT = "part-r-00000";

    public static final IntWritable INT_WRITABLE_ONE = new IntWritable(1);
    public static boolean isPositiveInteger(final String input) {
        return input.matches("^\\d+$");
    }
    public static boolean isPositiveFloat(final String input) {
        return input.matches("(^\\d+$)|(^\\d+\\.\\d+$)");
    }

    public static void copyFiles(String sourcePath, String destPath) throws IOException {
        FileChannel source=new FileInputStream(new File(sourcePath)).getChannel();
        FileChannel desti=new FileOutputStream(new File(destPath)).getChannel();
        desti.transferFrom(source, 0, source.size());
        source.close();
        desti.close();
    }
}
