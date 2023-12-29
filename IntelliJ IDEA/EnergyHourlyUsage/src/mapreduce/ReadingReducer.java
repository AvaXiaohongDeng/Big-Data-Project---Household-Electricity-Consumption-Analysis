package mapreduce;

import mapreduce.MaxMinReading;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;


public class ReadingReducer extends Reducer<Text, MaxMinReading, Text, FloatWritable> {

    @Override
    protected void reduce(Text key, Iterable<MaxMinReading> maxMinReading,
                          Context context)
            throws IOException, InterruptedException {
        float minReading = 999999999F;
        float maxReading = 0.0F;
        float usage = 0.0F;

        for (MaxMinReading mm: maxMinReading) {
            minReading = Math.min(minReading, mm.getMinReading());
            maxReading = Math.max(maxReading, mm.getMaxReading());
        }
        usage = maxReading - minReading;
        context.write(key, new FloatWritable(usage));
    }
}