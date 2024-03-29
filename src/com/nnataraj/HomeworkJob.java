
/*
 * HomeworkJob.java
 *
 * Created on Oct 22, 2012, 5:38:18 PM
 */

package com.nnataraj;


// import org.apache.commons.logging.Log;
// import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;

/**
 * @author nagaprasad
 */
public class HomeworkJob {
    // The Karmasphere Studio Workflow Log displays logging from Apache Commons Logging, for example:
    // private static final Log LOG = LogFactory.getLog("com.nnataraj.HomeworkJob");

    public static void main(String[] args) throws Exception {

        FileUtil.fullyDelete(new File(new Path(args[1] + "_temp").toString()));
        FileUtil.fullyDelete(new File(new Path(args[1]).toString()));

        Job job = new Job();

        /* Autogenerated initialization. */
        initJob(job, com.nnataraj.HomeworkMapper.class, com.nnataraj.HomeworkReducer.class);

        /* Custom initialization. */
        initCustom(job);

        /* Tell Task Tracker this is the main */
        job.setJarByClass(HomeworkJob.class);

        /* This is an example of how to set input and output. */
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1] + "_temp"));

        /* You can now do any other job customization. */
        // job.setXxx(...);

        /* And finally, we submit the job. */
        job.submit();

        if (job.waitForCompletion(true)) {
            job = new Job();

            /* Autogenerated initialization. */
            initJob(job, com.nnataraj.HomeworkMapper2.class, com.nnataraj.HomeworkReducer2.class);

            /* Custom initialization. */
            initCustom(job);

            /* Tell Task Tracker this is the main */
            job.setJarByClass(HomeworkJob.class);


            /* This is an example of how to set input and output. */
            FileInputFormat.setInputPaths(job, args[1] + "_temp");
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            /* You can now do any other job customization. */
            // job.setXxx(...);

            /* And finally, we submit the job. */
            job.submit();
        }
    }

    /**
     * This method is executed by the workflow
     */
    public static void initCustom(Job job) {
        // Add custom initialisation here, you may have to rebuild your project before
        // changes are reflected in the workflow.
    }

    /**
     * This method is called from within the constructor to
     * initialize the job.
     * WARNING: Do NOT modify this code. The content of this method is
     * always regenerated by the Job Editor.
     */
    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initJob
    public static void initJob(Job job, Class<? extends Mapper> mapperClass, Class<? extends Reducer> reducerClass) {
        org.apache.hadoop.conf.Configuration conf = job.getConfiguration();
// Generating code using Karmasphere Protocol for Hadoop 0.20
// CG_GLOBAL

// CG_INPUT_HIDDEN
        job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat.class);

// CG_MAPPER_HIDDEN
        job.setMapperClass(mapperClass);
        job.getConfiguration().set("mapred.mapper.new-api", "true");

// CG_MAPPER
        job.getConfiguration().set("mapred.map.tasks", "3");
        job.setMapOutputKeyClass(org.apache.hadoop.io.Text.class);
        job.setMapOutputValueClass(org.apache.hadoop.io.Text.class);

// CG_PARTITIONER_HIDDEN
        job.setPartitionerClass(org.apache.hadoop.mapreduce.lib.partition.HashPartitioner.class);

// CG_PARTITIONER

// CG_COMPARATOR_HIDDEN

// CG_COMPARATOR

// CG_COMBINER_HIDDEN

// CG_REDUCER_HIDDEN
        job.setReducerClass(reducerClass);
        job.getConfiguration().set("mapred.reducer.new-api", "true");

// CG_REDUCER
        job.getConfiguration().set("mapred.reduce.tasks", "2");
        job.setOutputKeyClass(org.apache.hadoop.io.Text.class);
        job.setOutputValueClass(org.apache.hadoop.io.Text.class);

// CG_OUTPUT_HIDDEN
        job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);

// CG_OUTPUT

// Others
        job.getConfiguration().set("", "");
    }

    // </editor-fold>//GEN-END:initJob

}
