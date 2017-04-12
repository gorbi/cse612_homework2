
/*
 * HomeworkMapper.java
 *
 * Created on Oct 22, 2012, 5:41:50 PM
 */

package com.nnataraj;


import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
// import org.apache.commons.logging.Log;
// import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.io.Text;

/**
 * @author nagaprasad
 */
public class HomeworkMapper extends Mapper<Text, Text, Text, Text> {
    // The Karmasphere Studio Workflow Log displays logging from Apache Commons Logging, for example:
    // private static final Log LOG = LogFactory.getLog("com.nnataraj.HomeworkMapper");

    @Override
    protected void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {
        context.write(key, value);
    }
}
