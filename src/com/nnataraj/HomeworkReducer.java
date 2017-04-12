
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

    private Map<String, List<String>> graph = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        List<String> values_ = new ArrayList<>();
        for (Text value : values) {
            values_.add(value.toString());
        }

        graph.put(key.toString(), values_);

        for (String value : values_) {
            if (graph.containsKey(value)) {
                for (String node : graph.get(value)) {
                    if (!key.toString().equalsIgnoreCase(node)) {
                        context.write(new Text(node + "," + value), key);
                    }
                }
            }
            Set<String> keys = graph.keySet();
            for (String key_ : keys) {
                if (!key_.equalsIgnoreCase(key.toString()))
                    if (graph.get(key_).contains(key.toString())) {
                        if (!value.equalsIgnoreCase(key_))
                            context.write(new Text(value + "," + key), new Text(key_));
                    }
            }
        }


    }
}
