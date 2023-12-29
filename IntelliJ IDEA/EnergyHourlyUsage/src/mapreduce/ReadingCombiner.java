package mapreduce;

import mapreduce.MaxMinReading;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReadingCombiner extends Reducer<Text, MaxMinReading, Text, MaxMinReading> {
    @Override
    protected void reduce(Text key, Iterable<MaxMinReading> maxMinReading,
                          Context context) throws IOException, InterruptedException {
        float minReading = 999999999F;
        float maxReading = 0.0F;
        for (MaxMinReading m: maxMinReading) {
            minReading = Math.min(minReading,m.getMinReading());
            maxReading = Math.max(maxReading,m.getMaxReading());
        }
        context.write(key, new MaxMinReading(maxReading,minReading));
    }
}
