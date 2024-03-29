/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.webank.defibus.broker.client;

import com.webank.defibus.broker.DeFiBrokerController;
import com.webank.defibus.broker.consumequeue.ConsumeQueueAccessLockManager;
import com.webank.defibus.common.DeFiBusBrokerConfig;
import com.webank.defibus.common.util.ReflectUtil;
import io.netty.channel.Channel;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupEvent;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.broker.client.ConsumerIdsChangeListener;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeFiConsumerManager extends ConsumerManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final long CHANNEL_EXPIRED_TIMEOUT = 1000 * 120;
    private ConcurrentHashMap<String/* Group */, ConsumerGroupInfo> consumerTable =
        new ConcurrentHashMap<String, ConsumerGroupInfo>(1024);
    private final ConsumerIdsChangeListener consumerIdsChangeListener;
    private final AdjustQueueNumStrategy adjustQueueNumStrategy;
    private final ExecutorService notifyClientExecutor;
    private final BlockingQueue<Runnable> notifyClientThreadPoolQueue;
    private final DeFiBrokerController deFiBrokerController;

    private final ConcurrentHashMap<String, String> dedupMapForNotifyClientChange = new ConcurrentHashMap<>();
    private final String dedupKeyPrefixForNotifyClientChange = "NCC$";

    @SuppressWarnings("unchecked")
    public DeFiConsumerManager(final ConsumerIdsChangeListener consumerIdsChangeListener,
        final AdjustQueueNumStrategy strategy, DeFiBrokerController deFiBrokerController) {
        super(consumerIdsChangeListener);
        this.consumerIdsChangeListener = consumerIdsChangeListener;
        this.adjustQueueNumStrategy = strategy;
        this.deFiBrokerController = deFiBrokerController;
        DeFiBusBrokerConfig deFiBusBrokerConfig =deFiBrokerController.getDeFiBusBrokerConfig();
        this.notifyClientThreadPoolQueue = new LinkedBlockingQueue<Runnable>(deFiBusBrokerConfig.getNotifyClientThreadPoolQueueCapacity());
        this.notifyClientExecutor = new ThreadPoolExecutor(
            deFiBusBrokerConfig.getNotifyClientThreadPoolNums(),
            deFiBusBrokerConfig.getNotifyClientThreadPoolNums(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.notifyClientThreadPoolQueue,
            new ThreadFactoryImpl("notifyClientThread_"));

        try {
            this.consumerTable = (ConcurrentHashMap<String, ConsumerGroupInfo>) ReflectUtil.getSimpleProperty(ConsumerManager.class, this, "consumerTable");
        } catch (Exception ex) {
            log.warn("init DeFiConsumerManager err.", ex);
        }
    }

    @Override
    public boolean registerConsumer(final String group, final ClientChannelInfo clientChannelInfo,
        ConsumeType consumeType, MessageModel messageModel, ConsumeFromWhere consumeFromWhere,
        final Set<SubscriptionData> subList, boolean isNotifyConsumerIdsChangedEnable) {
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(group);
        if (null == consumerGroupInfo) {
            ConsumerGroupInfo tmp = new DeFiConsumerGroupInfo(group, consumeType, messageModel, consumeFromWhere);
            ConsumerGroupInfo prev = this.consumerTable.putIfAbsent(group, tmp);
            consumerGroupInfo = prev != null ? prev : tmp;
        }
        DeFiConsumerGroupInfo deFiConsumerGroupInfo = (DeFiConsumerGroupInfo) consumerGroupInfo;

        Set<String> oldSub = deFiConsumerGroupInfo.findSubscribedTopicByClientId(clientChannelInfo.getClientId());
        boolean r1 = super.registerConsumer(group, clientChannelInfo, consumeType, messageModel, consumeFromWhere, subList, isNotifyConsumerIdsChangedEnable);
        boolean r2 = deFiConsumerGroupInfo.registerClientId(subList, clientChannelInfo.getClientId());

        if (r1 || r2) {
            adjustQueueNum(oldSub, subList);
            if (isNotifyConsumerIdsChangedEnable) {
//                this.consumerIdsChangeListener.handle(ConsumerGroupEvent.CHANGE, group, consumerGroupInfo.getAllChannel());
                asyncNotifyClientChange(group, deFiConsumerGroupInfo);
            }
        }

        this.consumerIdsChangeListener.handle(ConsumerGroupEvent.REGISTER, group, subList);
        return r1 || r2;
    }

    @Override
    public void unregisterConsumer(final String group, final ClientChannelInfo clientChannelInfo,
        boolean isNotifyConsumerIdsChangedEnable) {
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(group);
        Set<String> subscribeTopics = null;
        if (null != consumerGroupInfo) {
            DeFiConsumerGroupInfo deFiConsumerGroupInfo = (DeFiConsumerGroupInfo) consumerGroupInfo;
            this.removeAccessLock(deFiConsumerGroupInfo,clientChannelInfo);
            subscribeTopics = deFiConsumerGroupInfo.unregisterClientId(clientChannelInfo);
        }
        super.unregisterConsumer(group, clientChannelInfo, isNotifyConsumerIdsChangedEnable);

        if (subscribeTopics != null) {
            for (String topic : subscribeTopics) {
                adjustQueueNumStrategy.decreaseQueueNum(topic);
            }
        }
    }

    public void removeAccessLock(DeFiConsumerGroupInfo info,ClientChannelInfo clientChannelInfo) {
        ConsumeQueueAccessLockManager accessLockManager = this.deFiBrokerController.getMqAccessLockManager();
        String group = info.getGroupName();
        String clientId = clientChannelInfo.getClientId();
        for (Map.Entry<String, CopyOnWriteArraySet<String>> subEntry:info.getClientIdMap().entrySet()) {
            String topic = subEntry.getKey();
            if (subEntry.getValue().contains(clientId)) {
                accessLockManager.removeAccessLock(group,topic,clientId);
            }
        }
    }

    @Override
    public void doChannelCloseEvent(final String remoteAddr, final Channel channel) {
        Set<String> subscribeTopics = null;
        Iterator<Map.Entry<String, ConsumerGroupInfo>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, ConsumerGroupInfo> next = it.next();
            ConsumerGroupInfo info = next.getValue();
            String group = next.getKey();
            DeFiConsumerGroupInfo deFiConsumerGroupInfo = (DeFiConsumerGroupInfo) info;
            if (info.getChannelInfoTable().get(channel) != null) {
                ClientChannelInfo clientChannelInfo = info.getChannelInfoTable().get(channel);
                //移除clientId
                this.removeAccessLock(deFiConsumerGroupInfo,clientChannelInfo);
                subscribeTopics = deFiConsumerGroupInfo.unregisterClientId(clientChannelInfo);
                //缩Q
                if (subscribeTopics != null) {
                    for (String topic : subscribeTopics) {
                        adjustQueueNumStrategy.decreaseQueueNum(topic);
                    }
                }
                //移除channel
                boolean removed = info.doChannelCloseEvent(remoteAddr, channel);
                if (removed && info.getChannelInfoTable().isEmpty()) {
                    ConsumerGroupInfo remove = this.consumerTable.remove(group);
                    if (remove != null) {
                        log.info("unregister consumer ok, no connection in this group,remove consumer group from consumerTable, {}",
                            group);
                    }
                }
            }
            //异步通知clientId变化
            asyncNotifyClientChange(group, deFiConsumerGroupInfo);
        }
    }

    @Override
    public void scanNotActiveChannel() {
        Iterator<Map.Entry<String, ConsumerGroupInfo>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, ConsumerGroupInfo> next = it.next();
            String group = next.getKey();
            DeFiConsumerGroupInfo consumerGroupInfo = (DeFiConsumerGroupInfo) next.getValue();
            ConcurrentMap<Channel, ClientChannelInfo> channelInfoTable =
                consumerGroupInfo.getChannelInfoTable();

            Iterator<Map.Entry<Channel, ClientChannelInfo>> itChannel = channelInfoTable.entrySet().iterator();
            while (itChannel.hasNext()) {
                Map.Entry<Channel, ClientChannelInfo> nextChannel = itChannel.next();
                ClientChannelInfo clientChannelInfo = nextChannel.getValue();
                long diff = System.currentTimeMillis() - clientChannelInfo.getLastUpdateTimestamp();
                if (diff > CHANNEL_EXPIRED_TIMEOUT) {
                    log.warn(
                        "SCAN: remove expired channel from ConsumerManager consumerTable. channel={}, consumerGroup={}",
                        RemotingHelper.parseChannelRemoteAddr(clientChannelInfo.getChannel()), group);
                    RemotingUtil.closeChannel(clientChannelInfo.getChannel());
                    itChannel.remove();
                    this.removeAccessLock(consumerGroupInfo,clientChannelInfo);
                    Set<String> subscribeTopics = consumerGroupInfo.unregisterClientId(clientChannelInfo);
                    if (subscribeTopics != null) {
                        for (String topic : subscribeTopics) {
                            adjustQueueNumStrategy.decreaseQueueNum(topic);
                        }
                    }
                    //异步通知clientId变化
                    asyncNotifyClientChange(group, consumerGroupInfo);
                }
            }

            if (channelInfoTable.isEmpty()) {
                log.warn(
                    "SCAN: remove expired channel from ConsumerManager consumerTable, all clear, consumerGroup={}",
                    group);
                it.remove();
            }
            scanDirtyClientIdByGroup(consumerGroupInfo);
        }

    }

    private void asyncNotifyClientChange(final String group, DeFiConsumerGroupInfo deFiConsumerGroupInfo) {
        try {
            String dedupKey = dedupKeyPrefixForNotifyClientChange + group;
            String old = dedupMapForNotifyClientChange.putIfAbsent(dedupKey, group);
            if (StringUtils.isNotBlank(old)) {
                return;
            }
            //如果 dedupMap 中存在该group, 则表明存在 还未被执行的通知，可忽略本次
            //如果 dedupMap 中不存在该group, 则表明要么存在正在执行的通知, 要么不存在即将被执行的通知, 需要将本次排队
            this.notifyClientExecutor.execute(() -> {
                try {
                    dedupMapForNotifyClientChange.remove(dedupKey);
                    if (deFiConsumerGroupInfo == null) {
                        return;
                    }
                    consumerIdsChangeListener.handle(ConsumerGroupEvent.CHANGE, group, getNotifyClientChannel(deFiConsumerGroupInfo));
                } catch (Exception ex) {
                }
            });
        } catch (RejectedExecutionException re) {
            log.info("async notify client discard, group: {}", group);
        }
    }

    public List<Channel> getNotifyClientChannel(DeFiConsumerGroupInfo deFiConsumerGroupInfo) {
        long startTime = System.currentTimeMillis();
        List<Channel> result = new ArrayList<>();
        Set<String> notifyClientId = new HashSet<>();
        Iterator<Map.Entry<String, CopyOnWriteArraySet<String>>> it = deFiConsumerGroupInfo.getClientIdMap().entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, CopyOnWriteArraySet<String>> entry = it.next();
            TopicConfig topicConfig = this.deFiBrokerController.getTopicConfigManager().selectTopicConfig(entry.getKey());
            if (topicConfig != null) {
                notifyClientId.addAll(entry.getValue());
            }
        }
        Iterator<Map.Entry<Channel, ClientChannelInfo>> channelInfoIter = deFiConsumerGroupInfo.getChannelInfoTable().entrySet().iterator();
        while (channelInfoIter.hasNext()) {
            Map.Entry<Channel, ClientChannelInfo> entry = channelInfoIter.next();
            if (entry.getValue() != null && notifyClientId.contains(entry.getValue().getClientId())) {
                result.add(entry.getKey());
            }
        }

        List<Channel> activeChannels = new ArrayList<>();
        for (Channel chl : result) {
            if (chl.isActive()) {
                activeChannels.add(chl);
            }
        }

        if (result.size() > 0) {
            float activeRatio = (float) activeChannels.size() / result.size();
            if (activeRatio <= 0.5f) {
                log.info("inactive channel in group[{}] too much, activeChannels[{}], totalChannel[{}]",
                    deFiConsumerGroupInfo.getGroupName(), activeChannels.size(), result.size());
            }
        }

        long endTime = System.currentTimeMillis();
        if ((endTime - startTime) >= 5) {
            log.info("getNotifyClientChannel too long, time {} ms", (endTime - startTime));
        }
        return activeChannels;
    }

    private void adjustQueueNum(final Set<String> oldSub, final Set<SubscriptionData> subList) {
        for (SubscriptionData subscriptionData : subList) {
            if (!oldSub.contains(subscriptionData.getTopic())) {
                //new sub topic, increase queue num
                adjustQueueNumStrategy.increaseQueueNum(subscriptionData.getTopic());
            }
        }
        for (String topic : oldSub) {
            boolean stillSub = false;
            for (SubscriptionData subscriptionData : subList) {
                if (topic.equals(subscriptionData.getTopic())) {
                    stillSub = true;
                    break;
                }
            }
            if (!stillSub) {
                //no sub anymore, decrease queue num
                adjustQueueNumStrategy.decreaseQueueNum(topic);
            }
        }
    }

    public void notifyWhenTopicConfigChange(String topic) {
        adjustQueueNumStrategy.notifyWhenTopicConfigChange(topic);
    }

    public ConcurrentHashMap<String, ConsumerGroupInfo> getConsumerTable() {
        return this.consumerTable;
    }

    public ExecutorService getNotifyClientExecutor() {
        return notifyClientExecutor;
    }

    public BlockingQueue<Runnable> getNotifyClientThreadPoolQueue() {
        return notifyClientThreadPoolQueue;
    }

    public void scanDirtyClientIdByGroup(DeFiConsumerGroupInfo groupInfo) {
        if (groupInfo != null) {
            List<String> allChannelClientId = groupInfo.getAllClientId();
            Iterator<Map.Entry<String, CopyOnWriteArraySet<String>>> it = groupInfo.getClientIdMap().entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, CopyOnWriteArraySet<String>> next = it.next();
                Set<String> topicCid = new HashSet<>(next.getValue());
                for (String cid : topicCid) {
                    if (!allChannelClientId.contains(cid)) {
                        //check again to avoid mistaken
                        allChannelClientId = groupInfo.getAllClientId();
                        if (!allChannelClientId.contains(cid)) {
                            log.warn("SCAN DIRTY CLIENTID : {} in [{}] has no channel, maybe dirty. group: {} AllChannelClientId: {}", cid, next.getKey(), groupInfo.getGroupName(), allChannelClientId);
                            if (this.deFiBrokerController.getDeFiBusBrokerConfig().isAutoCleanDirtyClientId()) {
                                boolean removed = next.getValue().remove(cid);
                                if (removed) {
                                    log.info("remove dirty clientId {} from {} success. {}", cid, next.getKey(), groupInfo.getGroupName());
                                    this.consumerIdsChangeListener.handle(ConsumerGroupEvent.CHANGE, groupInfo.getGroupName(), getNotifyClientChannel(groupInfo));
                                } else {
                                    log.info("remove dirty clientId {} from {} fail. group: {} current cidList: {}", cid, next.getKey(), groupInfo.getGroupName(), next.getValue());
                                }
                            }
                        }
                    }
                }
            }
        }
    }

}
