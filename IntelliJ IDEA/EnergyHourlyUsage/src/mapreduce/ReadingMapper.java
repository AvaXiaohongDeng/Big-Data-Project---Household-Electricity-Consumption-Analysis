package mapreduce;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ReadingMapper extends Mapper<LongWritable, Text, Text, MaxMinReading> {
    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
        String[] values = value.toString().split("\t");
        String readingbyhousebydatebyhour;
        float reading;
        try {
            readingbyhousebydatebyhour =
                    values[1]+"\t"+values[2]+"\t"+values[3].substring(0,2);
            reading = Float.parseFloat(values[4]);
        }
        catch (Exception e){
            readingbyhousebydatebyhour = "9999";
            reading = 0.0F;
        }
        context.write(new Text(readingbyhousebydatebyhour),
                new MaxMinReading(reading, reading));
    }
}
