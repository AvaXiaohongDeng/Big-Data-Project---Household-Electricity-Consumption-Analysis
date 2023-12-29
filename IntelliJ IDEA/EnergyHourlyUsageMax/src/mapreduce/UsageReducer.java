package mapreduce;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class UsageReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    @Override
    protected void reduce(Text key, Iterable<FloatWritable> usage,
                          Context context)
            throws IOException, InterruptedException {
        float maxUsage= 0;
        for (FloatWritable u: usage) {
            maxUsage= Math.max(maxUsage, u.get());
        }
        context.write(key, new FloatWritable(maxUsage));
    }
}