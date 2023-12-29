package mapreduce;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class UsageMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
        String[] values = value.toString().split("\t");
        String usagebyhousebydatebyhour;
        float usage;
        try {
            usagebyhousebydatebyhour =values[0];
            usage= Float.parseFloat(values[3]);
        }
        catch (Exception e){
            usagebyhousebydatebyhour = "NA";
            usage = 0;
        }
        context.write(new Text(usagebyhousebydatebyhour), new FloatWritable(usage));
    }
}
