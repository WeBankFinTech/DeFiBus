package com.webank.defibus.broker.consumequeue;

import com.webank.defibus.broker.DeFiBrokerController;
import com.webank.defibus.common.protocol.body.MQLockInfo;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.common.constant.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumeQueueAccessLockManager {
    private final DeFiBrokerController brokerController;
    private static final Logger LOG = LoggerFactory.getLogger(LoggerName.REBALANCE_LOCK_LOGGER_NAME);

    //accessLockTable控制queue可以被哪个client监听
    private final ConcurrentHashMap<String/*Group*/, ConcurrentHashMap<String/*Topic*/, ConcurrentHashMap<Integer/*queueId*/, AccessLockEntry>>> accessLockTable = new ConcurrentHashMap<>();

    public ConsumeQueueAccessLockManager(DeFiBrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public synchronized boolean updateAccessControlTable(String group, String topic, String clientId, int queueId) {
        if (group != null && topic != null && clientId != null) {
            ConcurrentHashMap<String/*Topic*/, ConcurrentHashMap<Integer/*queueId*/, AccessLockEntry>> topicTable = accessLockTable.get(group);
            if (topicTable == null) {
                topicTable = new ConcurrentHashMap<>();
                accessLockTable.put(group, topicTable);
                LOG.info("group not exist, put group:{}", group);
            }
            ConcurrentHashMap<Integer/*queueId*/, AccessLockEntry> queueIdTable = topicTable.get(topic);
            if (queueIdTable == null) {
                queueIdTable = new ConcurrentHashMap<>();
                topicTable.put(topic, queueIdTable);
                LOG.info("topic not exist, put topic:{} into group {}", topic, group);
            }

            AccessLockEntry accessEntry = queueIdTable.get(queueId);
            if (accessEntry == null) {
                long deliverOffset = brokerController.getConsumeQueueManager().queryDeliverOffset(group, topic, queueId);
                accessEntry = new AccessLockEntry(clientId, System.currentTimeMillis(), deliverOffset);
                queueIdTable.put(queueId, accessEntry);
                LOG.info("mq is not locked. I got it. group:{}, topic:{}, queueId:{}, newClient:{}",
                    group, topic, queueId, clientId);
                return true;
            }

            //已经占有该Queue，则更新时间
            if (clientId.equals(accessEntry.getClientId())) {
                accessEntry.setLastAccessTimestamp(System.currentTimeMillis());
                accessEntry.setLastDeliverOffset(brokerController.getConsumeQueueManager().queryDeliverOffset(group, topic, queueId));
                return false;
            }
            //不占有该Queue，且不是wakeup的请求，才能抢锁
            else {
                long holdTimeThreshold = brokerController.getDeFiBusBrokerConfig().getLockQueueTimeout();
                long realHoldTime = System.currentTimeMillis() - accessEntry.getLastAccessTimestamp();
                boolean holdTimeout = (realHoldTime > holdTimeThreshold);

                long deliverOffset = brokerController.getConsumeQueueManager().queryDeliverOffset(group, topic, queueId);
                long lastDeliverOffset = accessEntry.getLastDeliverOffset();
                if (deliverOffset == lastDeliverOffset) {
                    accessEntry.getDeliverOffsetNoChangeTimes().incrementAndGet();
                } else {
                    accessEntry.setLastDeliverOffset(deliverOffset);
                    accessEntry.setDeliverOffsetNoChangeTimes(0);
                }

                long ackOffset = brokerController.getConsumeQueueManager().queryOffset(group, topic, queueId);
                long diff = deliverOffset - ackOffset;
                boolean offsetEqual = (diff == 0);

                int deliverOffsetNoChangeTimes = accessEntry.getDeliverOffsetNoChangeTimes().get();
                boolean deliverNoChange = (deliverOffsetNoChangeTimes >= brokerController.getDeFiBusBrokerConfig().getMaxDeliverOffsetNoChangeTimes());

                if ((offsetEqual && deliverNoChange) || holdTimeout) {
                    LOG.info("tryLock mq, update access lock table. topic:{}, queueId:{}, newClient:{}, oldClient:{}, hold time threshold:{}, real hold time:{}, hold timeout:{}, offset equal:{}, diff:{}, deliverOffset no change:{}, deliverOffset:{}, ackOffset:{}",
                        topic,
                        queueId,
                        clientId,
                        accessEntry.getClientId(),
                        holdTimeThreshold,
                        realHoldTime,
                        holdTimeout,
                        offsetEqual,
                        diff,
                        deliverNoChange,
                        deliverOffset,
                        ackOffset);

                    accessEntry.setLastAccessTimestamp(System.currentTimeMillis());
                    accessEntry.setLastDeliverOffset(deliverOffset);
                    accessEntry.getDeliverOffsetNoChangeTimes().set(0);
                    accessEntry.setClientId(clientId);
                    return true;
                }
                LOG.info("tryLock mq, but mq locked by other client: {}, group: {}, topic: {}, queueId: {}, nowClient:{}, hold timeout:{}, offset equal:{}, deliverOffset no change times:{}", accessEntry.getClientId(),
                    group, topic, queueId, clientId, holdTimeout, offsetEqual, deliverOffsetNoChangeTimes);
                return false;
            }

        }
        return false;
    }

    public boolean isAccessAllowed(String consumerGroup, String topic, String clientId, int queueId) {
        if (accessLockTable.get(consumerGroup) != null &&
            accessLockTable.get(consumerGroup).get(topic) != null) {
            AccessLockEntry accessEntry = accessLockTable.get(consumerGroup).get(topic).get(queueId);
            if (accessEntry != null && accessEntry.getClientId() != null && accessEntry.getClientId().equals(clientId)) {
                return true;
            }
        }
        return false;
    }

    public synchronized void removeAccessLock(String group, String topic, String clientId) {
        if (accessLockTable.get(group) != null && accessLockTable.get(group).get(topic) != null) {
            ConcurrentHashMap<Integer, AccessLockEntry> queueIdMap = accessLockTable.get(group).get(topic);
            Iterator<Map.Entry<Integer, AccessLockEntry>> it = queueIdMap.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Integer, AccessLockEntry> entry = it.next();
                if (entry != null && entry.getValue() != null && entry.getValue().getClientId().equals(clientId)) {
                    it.remove();
                    LOG.info("unlock mq. group: {}, topic: {}, queueId: {}, client: {}", group, topic, entry.getKey(), clientId);
                }
            }
            if (queueIdMap.size() <= 0) {
                accessLockTable.get(group).remove(topic);
                LOG.info("unlock mq. no client lock mq of topic {}, remove topic from map.", topic);
            }
            if (accessLockTable.get(group).size() <= 0) {
                accessLockTable.remove(group);
                LOG.info("unlock mq. no client lock mq of group {}, remove group from map.", group);
            }
        }
    }

    private void printACT() {
        for (String group : accessLockTable.keySet()) {
            LOG.info("group:{}", group);
            for (String topic : accessLockTable.get(group).keySet()) {
                LOG.info("topic:{}", topic);
                for (Integer qId : accessLockTable.get(group).get(topic).keySet()) {
                    LOG.info("queueId:{}, [{}, {}]", qId, accessLockTable.get(group).get(topic).get(qId).getClientId(),
                        accessLockTable.get(group).get(topic).get(qId).getLastAccessTimestamp());
                }
            }
        }
    }

    public Set<MQLockInfo> getLockResultByTopic(String group, String topic) {
        Set<MQLockInfo> result = new HashSet<>();
        if (accessLockTable.get(group) != null && accessLockTable.get(group).get(topic) != null) {
            Iterator<Map.Entry<Integer, AccessLockEntry>> it = accessLockTable.get(group).get(topic).entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Integer, AccessLockEntry> entry = it.next();
                MQLockInfo lockInfo = new MQLockInfo();
                lockInfo.setQueueId(entry.getKey());
                lockInfo.setClientId(entry.getValue().getClientId());
                lockInfo.setLastAccessTimestamp(entry.getValue().getLastAccessTimestamp());
                lockInfo.setLastDeliverOffset(entry.getValue().getLastDeliverOffset());
                lockInfo.setDeliverOffsetNoChangeTimes(entry.getValue().getDeliverOffsetNoChangeTimes().get());
                result.add(lockInfo);
            }
        }
        return result;
    }

    static class AccessLockEntry {
        String clientId;
        long lastAccessTimestamp;
        long lastDeliverOffset;
        AtomicInteger deliverOffsetNoChangeTimes;

        public AccessLockEntry(String clientId, long lastAccessTimestamp, long lastDeliverOffset) {
            this.clientId = clientId;
            this.lastAccessTimestamp = lastAccessTimestamp;
            this.lastDeliverOffset = lastDeliverOffset;
            deliverOffsetNoChangeTimes = new AtomicInteger(0);
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

        public AtomicInteger getDeliverOffsetNoChangeTimes() {
            return deliverOffsetNoChangeTimes;
        }

        public void setDeliverOffsetNoChangeTimes(int deliverOffsetNoChangeTimes) {
            this.deliverOffsetNoChangeTimes.set(deliverOffsetNoChangeTimes);
        }
    }
}