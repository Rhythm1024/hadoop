package com.mr.LastWeek;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Administrator on 2018/01/18.
 */
public class FlowOrder implements WritableComparable<FlowOrder> {
    private String phoneNumber = "";
    private int upFlow;
    private int downFlow;
    private int allFlow;

    public String getPhoneNumber() {
        return phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

    public int getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(int upFlow) {
        this.upFlow = upFlow;
    }

    public int getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(int downFlow) {
        this.downFlow = downFlow;
    }

    public int getAllFlow() {
        return upFlow+downFlow;
    }

    public void setAllFlow(int allFlow) {
        this.allFlow = allFlow;
    }

    @Override
    public int compareTo(FlowOrder o) {
        if (o.allFlow == this.allFlow) {
            return o.downFlow - this.downFlow;
        }
        return o.allFlow - this.allFlow;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(phoneNumber);
        dataOutput.writeInt(upFlow);
        dataOutput.writeInt(downFlow);
        dataOutput.writeInt(allFlow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.phoneNumber = dataInput.readUTF();
        this.upFlow = dataInput.readInt();
        this.downFlow = dataInput.readInt();
        this.allFlow = dataInput.readInt();
    }

    @Override
    public String toString() {
        return "FlowOrder{" +
                "phoneNumber='" + phoneNumber + '\'' +
                ", upFlow=" + upFlow +
                ", downFlow=" + downFlow +
                ", allFlow=" + allFlow +
                '}';
    }
}