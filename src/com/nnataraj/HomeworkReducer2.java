
/*
 * HomeworkReducer.java
 *
 * Created on Oct 22, 2012, 5:46:32 PM
 */

package com.nnataraj;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

// import org.apache.commons.logging.Log;
// import org.apache.commons.logging.LogFactory;

/**
 * @author nagaprasad
 */
public class HomeworkReducer2 extends Reducer<Text, Text, Text, Text> {
    // The Karmasphere Studio Workflow Log displays logging from Apache Commons Logging, for example:
    // private static final Log LOG = LogFactory.getLog("com.nnataraj.HomeworkReducer2");


    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        StringBuffer values_ = new StringBuffer();
        for (Text value : values)
            values_.append(value + ",");
        values_.replace(values_.length() - 1, values_.length(), "");

        context.write(key, new Text(values_.toString()));
    }
}
