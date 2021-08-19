package com.webank.defibus.common.protocol.body;

public class MQLockInfo {
    private int queueId;
    private String clientId;
    private long lastAccessTimestamp;
    private long lastDeliverOffset;
    private long deliverOffsetNoChangeTimes;

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public long getLastAccessTimestamp() {
        return lastAccessTimestamp;
    }

    public void setLastAccessTimestamp(long lastAccessTimestamp) {
        this.lastAccessTimestamp = lastAccessTimestamp;
    }

    public long getLastDeliverOffset() {
        return lastDeliverOffset;
    }

    public void setLastDeliverOffset(long lastDeliverOffset) {
        this.lastDeliverOffset = lastDeliverOffset;
    }

    public long getDeliverOffsetNoChangeTimes() {
        return deliverOffsetNoChangeTimes;
    }

    public void setDeliverOffsetNoChangeTimes(long deliverOffsetNoChangeTimes) {
        this.deliverOffsetNoChangeTimes = deliverOffsetNoChangeTimes;
    }
}
