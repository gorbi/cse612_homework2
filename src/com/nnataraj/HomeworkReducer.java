
/*
 * HomeworkReducer.java
 *
 * Created on Oct 22, 2012, 5:46:32 PM
 */

package com.nnataraj;


import java.io.IOException;
import java.util.*;
// import org.apache.commons.logging.Log;
// import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.bloom.Key;

/**
 * @author nagaprasad
 */
public class HomeworkReducer extends Reducer<Text, Text, Text, Text> {
    // The Karmasphere Studio Workflow Log displays logging from Apache Commons Logging, for example:
    // private static final Log LOG = LogFactory.getLog("com.nnataraj.HomeworkReducer");

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        MapList mapList = new MapList();

        for (Text value : values) {
            String[] kv = value.toString().split(",");
            mapList.put(kv[0], kv[1]);
        }

        Set<String> keys = mapList.keySet();
        for (String key_ : keys) {
            List<String> values_ = mapList.get(key_);
            for (String value : values_) {
                if (mapList.containsKey(value)) {
                    List<String> values__ = mapList.get(value);
                    for (String value_ : values__) {
                        if (!key_.equals(value_))
                            context.write(new Text(value_ + "," + value), new Text(key_));
                    }
                }
            }
        }


    }
}
