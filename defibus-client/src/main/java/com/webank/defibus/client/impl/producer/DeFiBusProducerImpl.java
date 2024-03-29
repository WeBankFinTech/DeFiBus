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

package com.webank.defibus.client.impl.producer;

import com.webank.defibus.client.common.DeFiBusClientConfig;
import com.webank.defibus.client.impl.factory.DeFiBusClientInstance;
import com.webank.defibus.common.DeFiBusConstant;
import com.webank.defibus.common.DeFiBusErrorCode;
import com.webank.defibus.common.exception.DeFiBusException;
import com.webank.defibus.common.protocol.DeFiBusResponseCode;
import com.webank.defibus.common.util.DeFiBusRequestIDUtil;
import com.webank.defibus.producer.DeFiBusProducer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeFiBusProducerImpl {
    public static final Logger LOGGER = LoggerFactory.getLogger(DeFiBusProducerImpl.class);

    private DeFiBusProducer deFiBusProducer;
    private HealthyMessageQueueSelector messageQueueSelector;
    private ScheduledExecutorService scheduledExecutorService;

    private ScheduledExecutorService healthyMessageQueueDetector;
    private ExecutorService executorService = null;
    private ConcurrentHashMap<String, Boolean> topicInitMap = new ConcurrentHashMap<String, Boolean>();
    private ClusterInfo clusterInfo;

    private long InnerTimeoutMax = 1414;
    private long InnerTimeoutMin = 100;

    public DeFiBusProducerImpl(DeFiBusProducer deFiBusProducer, DeFiBusClientConfig deFiBusClientConfig,
                               DeFiBusClientInstance deFiBusClientInstance) {
        this.deFiBusProducer = deFiBusProducer;
        this.messageQueueSelector = new HealthyMessageQueueSelector(new MessageQueueHealthManager(deFiBusClientConfig.getQueueIsolateTimeMillis()),
                deFiBusClientConfig.getMinMqNumWhenSendLocal());

        executorService = deFiBusClientInstance.getExecutorService();
        scheduledExecutorService = deFiBusClientInstance.getScheduledExecutorService();

        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                cleanExpiredRRRequest();
            }
        }, 0, 1000, TimeUnit.MILLISECONDS);

        if (deFiBusClientConfig.isDetectHealthyMessageQueueEnable()) {
            healthyMessageQueueDetector = Executors.newSingleThreadScheduledExecutor(
                    r -> {
                        Thread t = new Thread("HealthyMessageQueueDetectThread");
                        return t;
                    }
            );
            healthyMessageQueueDetector.scheduleWithFixedDelay(this::checkMessageQueue, 3000, 30000, TimeUnit.MILLISECONDS);
        }
    }

    private boolean isBrokerReachable(String brokerName) {
        try {
            String brokerAddr = deFiBusProducer.getDefaultMQProducer().getDefaultMQProducerImpl().getmQClientFactory().findBrokerAddressInPublish(brokerName);
            if (brokerAddr == null) {
                return false;
            }
            boolean reachable = deFiBusProducer.getDeFiBusClientInstance().brokerHealth(brokerAddr, 3000);
            if (!reachable) {
                LOGGER.warn("broker is not reachable, brokerName:{},  brokerAddr:{},", brokerName, brokerAddr);
            }
            return reachable;
        } catch (Exception e) {
            LOGGER.warn("isBrokerReachable exception,brokerName:{}", brokerName, e);
            return false;
        }
    }

    public void checkMessageQueue() {
        try {
            MessageQueueHealthManager manager = this.messageQueueSelector.getMessageQueueHealthManager();
            ConcurrentHashMap<MessageQueue, Long> faultMap = manager.getFaultMap();

            Map<String, Boolean> checkedBrokers = new HashMap<>();
            for (Map.Entry<MessageQueue, Long> entry : faultMap.entrySet()) {
                String brokerName = entry.getKey().getBrokerName();
                if (StringUtils.isBlank(brokerName)) {
                    continue;
                }
                if (!checkedBrokers.containsKey(brokerName)) {
                    boolean reachable = isBrokerReachable(brokerName);
                    checkedBrokers.put(brokerName, reachable);
                }

                if (!checkedBrokers.get(brokerName)) {
                    manager.markQueueFault(entry.getKey());
                }
            }
            LOGGER.info("checkMessageQueue result: {}", checkedBrokers);
        } catch (Exception e) {
            LOGGER.warn("checkMessageQueue exception :", e);
        }
    }

    private void cleanExpiredRRRequest() {
        try {
            List<RRResponseFuture> expiredRRRequest = new ArrayList<RRResponseFuture>();

            Iterator<Map.Entry<String, RRResponseFuture>> it = ResponseTable.getRrResponseFurtureConcurrentHashMap().entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, RRResponseFuture> entry = it.next();
                String rId = entry.getKey();
                RRResponseFuture responseFurture = entry.getValue();
                if (responseFurture.getExpiredTime() + 1000L <= System.currentTimeMillis()) {
                    it.remove();
                    expiredRRRequest.add(responseFurture);
                    LOGGER.warn("remove timeout request " + rId);
                }
            }

            for (final RRResponseFuture responseFuture : expiredRRRequest) {
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        if (!responseFuture.release()) {
                            Throwable throwable = new DeFiBusException(DeFiBusErrorCode.RR_REQUEST_TIMEOUT, "remove timeout request, deadline: " + responseFuture.getExpiredTime());
                            responseFuture.getRrCallback().onException(throwable);
                        }
                    }
                });
            }
        } catch (Throwable ignore) {
            LOGGER.warn("cleanExpiredRRRequest failed ,{}", ignore.getMessage());
        }
    }

    public void reply(
            Message replyMsg,
            final SendCallback sendCallback) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        replyMsg.putUserProperty(DeFiBusConstant.KEY, DeFiBusConstant.REPLY);
        replyMsg.putUserProperty(DeFiBusConstant.PROPERTY_MESSAGE_TTL, String.valueOf(deFiBusProducer.getDefaultMQProducer().getSendMsgTimeout()));

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Reply message: {} ", replyMsg.toString());
        }
        final String requestId = replyMsg.getUserProperty(DeFiBusConstant.PROPERTY_RR_REQUEST_ID);
        if (requestId == null) {
            LOGGER.warn("rr request id is null, can not reply");
        }

        long timeout = calculateInnerPublishTimeout(deFiBusProducer.getDefaultMQProducer().getSendMsgTimeout());
        publish(replyMsg, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                if (sendCallback != null) {
                    sendCallback.onSuccess(sendResult);
                }
            }

            @Override
            public void onException(Throwable e) {
                LOGGER.warn("Reply message fail, requestId={}", requestId);
                if (sendCallback != null) {
                    sendCallback.onException(e);
                }
            }
        }, timeout);
    }

    public Message request(Message requestMsg,
                           long timeout) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        return request(requestMsg, null, null, timeout);
    }

    public Message request(Message requestMsg, final SendCallback sendCallback, RRCallback rrCallback, long timeout)
            throws InterruptedException, RemotingException, MQClientException, MQBrokerException {

        boolean isAsyncRR = (rrCallback != null);

        final String uniqueRequestId = DeFiBusRequestIDUtil.createUniqueName("w");
        DefaultMQProducer producer = deFiBusProducer.getDefaultMQProducer();
        requestMsg.putUserProperty(DeFiBusConstant.KEY, DeFiBusConstant.PERSISTENT);
        requestMsg.putUserProperty(DeFiBusConstant.PROPERTY_RR_REQUEST_ID, uniqueRequestId);
        requestMsg.putUserProperty(DeFiBusConstant.PROPERTY_MESSAGE_REPLY_TO, producer.buildMQClientId());
        requestMsg.putUserProperty(DeFiBusConstant.PROPERTY_MESSAGE_TTL, String.valueOf(timeout));

        final RRResponseFuture responseFurture = new RRResponseFuture(rrCallback, timeout);

        String topic = requestMsg.getTopic();
        boolean hasRouteData = deFiBusProducer.getDefaultMQProducer().getDefaultMQProducerImpl().getmQClientFactory().getTopicRouteTable().containsKey(topic);
        Boolean isSendHeartbeatOk = topicInitMap.get(topic);
        if (isSendHeartbeatOk == null) {
            isSendHeartbeatOk = false;
        }
        if (!hasRouteData || !isSendHeartbeatOk) {
            long startTimestamp = System.currentTimeMillis();
            synchronized (this) {
                boolean hasRouteDataSync = deFiBusProducer.getDefaultMQProducer().getDefaultMQProducerImpl().getmQClientFactory().getTopicRouteTable().containsKey(topic);
                if (!hasRouteDataSync) {
                    LOGGER.info("no topic route info for " + topic + ", send heartbeat to nameserver");
                    deFiBusProducer.getDefaultMQProducer().getDefaultMQProducerImpl().getmQClientFactory().updateTopicRouteInfoFromNameServer(topic);
                    deFiBusProducer.getDefaultMQProducer().getDefaultMQProducerImpl().getmQClientFactory().sendHeartbeatToAllBrokerWithLock();
                    topicInitMap.put(topic, true);
                }
            }
            long cost = System.currentTimeMillis() - startTimestamp;
            if (cost > 500) {
                LOGGER.warn("get topic route info for {} before request cost {} ms.", topic, cost);
            }
        }

        long publishTimeout = calculateInnerPublishTimeout(timeout);

        ResponseTable.getRrResponseFurtureConcurrentHashMap().put(uniqueRequestId, responseFurture);
        if (isAsyncRR) {
            this.publish(requestMsg, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    if (sendCallback != null) {
                        sendCallback.onSuccess(sendResult);
                    }
                }

                @Override
                public void onException(Throwable e) {
                    LOGGER.warn("except when publish async rr message, uniqueId :{} {} ", uniqueRequestId, e.getMessage());
                    ResponseTable.getRrResponseFurtureConcurrentHashMap().remove(uniqueRequestId);
                    if (sendCallback != null) {
                        sendCallback.onException(e);
                    }
                }
            }, publishTimeout);
            return null;

        } else {
            publish(requestMsg, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    if (sendCallback != null) {
                        sendCallback.onSuccess(sendResult);
                    }
                }

                @Override
                public void onException(Throwable e) {
                    LOGGER.warn("except when publish sync rr message, uniqueId :{} {}", uniqueRequestId, e.getMessage());
                    ResponseTable.getRrResponseFurtureConcurrentHashMap().remove(uniqueRequestId);
                    if (sendCallback != null) {
                        sendCallback.onException(e);
                    }
                }
            }, publishTimeout);
            Message retMessage = responseFurture.waitResponse(timeout);
            ResponseTable.getRrResponseFurtureConcurrentHashMap().remove(uniqueRequestId);
            if (retMessage == null) {
                LOGGER.warn("request {} is sent, constant is :{}, but no rr response ", topic, uniqueRequestId);
            }
            return retMessage;
        }
    }

    public void publish(Message msg) throws MQClientException, RemotingException, InterruptedException {
        publish(msg, deFiBusProducer.getDefaultMQProducer().getSendMsgTimeout());
    }

    public void publish(Message msg, long timeout) throws MQClientException, RemotingException, InterruptedException {
        publish(msg, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                LOGGER.debug(sendResult.toString());
            }

            @Override
            public void onException(Throwable e) {
                LOGGER.warn("", e);
            }
        }, timeout);
    }

    public void publish(
            Collection<Message> msgs) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        for (Message msg : msgs) {
            if (msg.getUserProperty(DeFiBusConstant.PROPERTY_MESSAGE_TTL) == null) {
                msg.putUserProperty(DeFiBusConstant.PROPERTY_MESSAGE_TTL, DeFiBusConstant.DEFAULT_TTL);
            }
        }
        publish(batch(msgs));
    }

    private MessageBatch batch(Collection<Message> msgs) throws MQClientException {
        MessageBatch msgBatch;
        try {
            msgBatch = MessageBatch.generateFromList(msgs);
            for (Message message : msgBatch) {
                Validators.checkMessage(message, deFiBusProducer.getDefaultMQProducer());
                MessageClientIDSetter.setUniqID(message);
            }
            msgBatch.setBody(msgBatch.encode());
        } catch (Exception e) {
            throw new MQClientException("Failed to initiate the MessageBatch", e);
        }
        return msgBatch;
    }

    public void publish(Message msg,
                        SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException {
        publish(msg, sendCallback, this.deFiBusProducer.getDefaultMQProducer().getSendMsgTimeout());
    }

    public void publish(final Message msg, final SendCallback sendCallback,
                        final long timeout) throws MQClientException, RemotingException, InterruptedException {
        if (msg.getUserProperty(DeFiBusConstant.PROPERTY_MESSAGE_TTL) == null) {
            msg.putUserProperty(DeFiBusConstant.PROPERTY_MESSAGE_TTL, DeFiBusConstant.DEFAULT_TTL);
        }

        final AtomicReference<MessageQueue> selectorArgs = new AtomicReference<MessageQueue>();
        AsynCircuitBreakSendCallBack asynCircuitBreakSendCallBack = new AsynCircuitBreakSendCallBack();
        asynCircuitBreakSendCallBack.setMsg(msg);
        asynCircuitBreakSendCallBack.setProducer(this.deFiBusProducer);
        asynCircuitBreakSendCallBack.setSelectorArg(selectorArgs);
        asynCircuitBreakSendCallBack.setSendCallback(sendCallback);
        asynCircuitBreakSendCallBack.setPublishTimeout(timeout);

        String topic = msg.getTopic();
        boolean hasRouteData = deFiBusProducer.getDefaultMQProducer().getDefaultMQProducerImpl().getmQClientFactory().getTopicRouteTable().containsKey(topic);
        if (!hasRouteData) {
            LOGGER.info("no topic route info for " + topic + ", send heartbeat to nameserver");
            deFiBusProducer.getDefaultMQProducer().getDefaultMQProducerImpl().getmQClientFactory().updateTopicRouteInfoFromNameServer(topic);
        }

        DeFiBusProducerImpl.this.deFiBusProducer.getDefaultMQProducer().send(msg, messageQueueSelector, selectorArgs, asynCircuitBreakSendCallBack, timeout);
    }

    class AsynCircuitBreakSendCallBack implements SendCallback {
        private Message msg;
        private DeFiBusProducer producer;
        private AtomicReference<MessageQueue> selectorArg;
        private SendCallback sendCallback;
        private AtomicInteger sendRetryTimes = new AtomicInteger(0);
        private AtomicInteger circuitBreakRetryTimes = new AtomicInteger(0);
        private int queueCount = 0;

        private long publishTimeout = 3000;

        public void setPublishTimeout(long publishTimeout) {
            this.publishTimeout = publishTimeout;
        }

        public void setProducer(DeFiBusProducer producer) {
            this.producer = producer;
        }

        public void setMsg(Message msg) {
            this.msg = msg;
        }

        public void setSelectorArg(AtomicReference<MessageQueue> selectorArg) {
            this.selectorArg = selectorArg;
        }

        public void setSendCallback(SendCallback sendCallback) {
            this.sendCallback = sendCallback;
        }

        @Override
        public void onSuccess(SendResult sendResult) {
            messageQueueSelector.getMessageQueueHealthManager().markQueueHealthy(sendResult.getMessageQueue());
            if (sendCallback != null) {
                sendCallback.onSuccess(sendResult);
            }
        }

        @Override
        public void onException(Throwable e) {
            try {
                MessageQueueHealthManager messageQueueHealthManager
                        = ((HealthyMessageQueueSelector) messageQueueSelector).getMessageQueueHealthManager();
                MessageQueue messageQueue = ((AtomicReference<MessageQueue>) selectorArg).get();
                if (messageQueue != null) {
                    messageQueueSelector.getMessageQueueHealthManager().markQueueFault(messageQueue);
                    if (messageQueueSelector.getMessageQueueHealthManager().isQueueFault(messageQueue)) {
                        LOGGER.warn("isolate send failed mq. {} cause: {}", messageQueue, e.getMessage());
                    }
                }
                //logic of fuse
                if (e.getMessage().contains("CODE: " + DeFiBusResponseCode.CONSUME_DIFF_SPAN_TOO_LONG)) {
                    //first retry initialize
                    if (queueCount == 0) {
                        List<MessageQueue> messageQueueList = producer.getDefaultMQProducer().getDefaultMQProducerImpl().getTopicPublishInfoTable()
                                .get(msg.getTopic()).getMessageQueueList();
                        queueCount = messageQueueList.size();
                        String clusterPrefix = deFiBusProducer.getDeFiBusClientConfig().getClusterPrefix();
                        if (!StringUtils.isEmpty(clusterPrefix)) {
                            for (MessageQueue mq : messageQueueList) {
                                if (messageQueueHealthManager.isQueueFault(mq)) {
                                    queueCount--;
                                }
                            }
                        }
                    }

                    int retryTimes = Math.min(queueCount, deFiBusProducer.getDeFiBusClientConfig().getRetryTimesWhenSendAsyncFailed());
                    if (circuitBreakRetryTimes.get() < retryTimes) {
                        int currentRetryTimes = circuitBreakRetryTimes.incrementAndGet();
                        LOGGER.warn("fuse:send to [{}] circuit break, retry no.[{}] times, msgKey:[{}]", messageQueue.toString(), currentRetryTimes, msg.getKeys());
                        msg.putUserProperty(DeFiBusConstant.RETRY_TIME, "#" + currentRetryTimes);
                        producer.getDefaultMQProducer().send(msg, messageQueueSelector, selectorArg, this, this.publishTimeout);
                        //no exception to client when retry
                        return;
                    } else {
                        LOGGER.warn("fuse:send to [{}] circuit break after retry {} times, msgKey:[{}]", messageQueue.toString(), retryTimes, msg.getKeys());
                    }
                } else {
                    int maxRetryTimes = producer.getDeFiBusClientConfig().getRetryTimesWhenSendAsyncFailed();
                    if (sendRetryTimes.get() < maxRetryTimes) {
                        int currentRetryTimes = sendRetryTimes.incrementAndGet();
                        LOGGER.info("send message fail, retry {} now, msgKey: {}, cause: {}", currentRetryTimes, msg.getKeys(), e.getMessage());
                        msg.putUserProperty(DeFiBusConstant.RETRY_TIME, "#" + currentRetryTimes);
                        producer.getDefaultMQProducer().send(msg, messageQueueSelector, selectorArg, this, this.publishTimeout);
                        return;
                    } else {
                        LOGGER.warn("send message fail, after retry {} times, msgKey:[{}]", maxRetryTimes, msg.getKeys());
                    }
                }

                if (sendCallback != null) {
                    sendCallback.onException(e);
                }
            } catch (Exception e1) {
                LOGGER.warn("onExcept fail", e1);
                if (sendCallback != null) {
                    sendCallback.onException(e);
                }
            }
        }
    }

    public void updateSendNearbyMapping(Map<String, Boolean> newMapping) {
        this.messageQueueSelector.setSendNearbyMapping(newMapping);
    }

    public void startUpdateClusterInfoTask() {
        updateClusterInfo();
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                updateClusterInfo();
            }
        }, 0, 60, TimeUnit.SECONDS);
    }

    private void updateClusterInfo() {
        try {
            MQClientInstance mqClientInstance = this.deFiBusProducer.getDefaultMQProducer().getDefaultMQProducerImpl().getmQClientFactory();
            if (mqClientInstance != null
                    && this.deFiBusProducer.getDefaultMQProducer().getDefaultMQProducerImpl().getServiceState() == ServiceState.RUNNING) {
                if (mqClientInstance.getMQClientAPIImpl() != null && mqClientInstance.getMQClientAPIImpl().getNameServerAddressList() != null
                        && mqClientInstance.getMQClientAPIImpl().getNameServerAddressList().size() == 0) {
                    mqClientInstance.getMQClientAPIImpl().fetchNameServerAddr();
                }
                clusterInfo = mqClientInstance.getMQClientAPIImpl().getBrokerClusterInfo(3000);
                updateLocalBrokers(clusterInfo);
            }
        } catch (Throwable e) {
            LOGGER.warn("updateClusterInfo failed, {}", e.getMessage());
        }
    }

    private void updateLocalBrokers(ClusterInfo clusterInfo) {
        if (clusterInfo != null) {
            String clusterPrefix = deFiBusProducer.getDeFiBusClientConfig().getClusterPrefix();
            HashMap<String, Set<String>> clusterAddrTable = clusterInfo.getClusterAddrTable();
            Set<String> currentBrokers = new HashSet<String>();
            for (Map.Entry<String, Set<String>> entry : clusterAddrTable.entrySet()) {
                String clusterName = entry.getKey();
                String clusterIdc = StringUtils.split(clusterName, DeFiBusConstant.IDC_SEPERATER)[0];
                if (StringUtils.isNotEmpty(clusterPrefix) && StringUtils.equalsIgnoreCase(clusterIdc, clusterPrefix)) {
                    currentBrokers.addAll(entry.getValue());
                }
            }
            if (!currentBrokers.equals(messageQueueSelector.getLocalBrokers())) {
                messageQueueSelector.setLocalBrokers(currentBrokers);
                LOGGER.info("localBrokers updated:  {} , clusterPrefix :{} ", currentBrokers, clusterPrefix);
            }
        }
    }

    private long calculateInnerPublishTimeout(long expectTTL) {
        //特殊值,容易区分
        long timeout = InnerTimeoutMax;
        if (expectTTL < InnerTimeoutMax) {
            timeout = expectTTL / 2;
        }
        if (expectTTL < InnerTimeoutMin) {
            timeout = InnerTimeoutMin;
        }
        return timeout;
    }
}
