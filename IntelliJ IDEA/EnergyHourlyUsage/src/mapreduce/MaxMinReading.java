package mapreduce;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MaxMinReading implements Writable {

    float maxReading;
    float minReading;

    public MaxMinReading(){
        this.maxReading= 0.0f;
        this.minReading = 0.0f;
    }

    public MaxMinReading(float val1, float val2) {
        this.maxReading = val1;
        this.minReading = val2;
    }

    public float getMaxReading() {
        return maxReading;
    }

    public float getMinReading() {
        return minReading;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(maxReading);
        dataOutput.writeFloat(minReading);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        maxReading = dataInput.readFloat();
        minReading = dataInput.readFloat();
    }
}
