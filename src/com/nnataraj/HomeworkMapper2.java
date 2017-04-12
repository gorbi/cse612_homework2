
/*
 * HomeworkMapper.java
 *
 * Created on Oct 22, 2012, 5:41:50 PM
 */

package com.nnataraj;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// import org.apache.commons.logging.Log;
// import org.apache.commons.logging.LogFactory;

/**
 * @author nagaprasad
 */
public class HomeworkMapper2 extends Mapper<Text, Text, Text, Text> {
    // The Karmasphere Studio Workflow Log displays logging from Apache Commons Logging, for example:
    // private static final Log LOG = LogFactory.getLog("com.nnataraj.HomeworkMapper2");

    @Override
    protected void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {
        context.write(key, value);
    }
}
