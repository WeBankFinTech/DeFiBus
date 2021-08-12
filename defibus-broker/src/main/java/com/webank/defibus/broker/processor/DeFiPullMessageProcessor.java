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

package com.webank.defibus.broker.processor;

import com.webank.defibus.broker.DeFiBrokerController;
import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.broker.processor.PullMessageProcessor;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.PullMessageResponseHeader;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.rocketmq.common.protocol.ResponseCode.NO_PERMISSION;
import static org.apache.rocketmq.common.protocol.ResponseCode.PULL_NOT_FOUND;
import static org.apache.rocketmq.common.protocol.ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST;
import static org.apache.rocketmq.common.protocol.ResponseCode.SUBSCRIPTION_NOT_EXIST;
import static org.apache.rocketmq.common.protocol.ResponseCode.SUBSCRIPTION_NOT_LATEST;

public class DeFiPullMessageProcessor extends PullMessageProcessor {
    private DeFiBrokerController deFiBrokerController;
    private static final Logger LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    public DeFiPullMessageProcessor(BrokerController brokerController) {
        super(brokerController);
        this.deFiBrokerController = (DeFiBrokerController) brokerController;
    }

    @Override
    public RemotingCommand processRequest(final ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final PullMessageRequestHeader requestHeader =
            (PullMessageRequestHeader) request.decodeCommandCustomHeader(PullMessageRequestHeader.class);
        ConsumerGroupInfo consumerGroupInfo = deFiBrokerController.getConsumerManager().getConsumerGroupInfo(requestHeader.getConsumerGroup());
        if (deFiBrokerController.getDeFiBusBrokerConfig().getMqAccessControlEnable() == 1) {
            //集群模式才进行访问表控制
            if (consumerGroupInfo != null && consumerGroupInfo.getMessageModel() == MessageModel.CLUSTERING) {
                ClientChannelInfo clientChannelInfo = consumerGroupInfo.getChannelInfoTable().get(ctx.channel());
                if (clientChannelInfo != null) {
                    String group = consumerGroupInfo.getGroupName();
                    String topic = requestHeader.getTopic();
                    int queueId = requestHeader.getQueueId();
                    String clientId = clientChannelInfo.getClientId();

                    boolean acquired = deFiBrokerController.getMqAccessLockManager().updateAccessControlTable(group, topic, clientId, queueId);
                    boolean isAllowed = deFiBrokerController.getMqAccessLockManager().isAccessAllowed(group,topic,clientId,queueId);

                    //不是分给自己的Queue，返回空
                    if (!isAllowed) {
                        RemotingCommand response = RemotingCommand.createResponseCommand(PullMessageResponseHeader.class);
                        final PullMessageResponseHeader responseHeader = (PullMessageResponseHeader) response.readCustomHeader();

                        LOG.info("pull message rejected. queue is locked by other client. group:{}, topic:{}, queueId:{}, queueOffset:{}, request clientId:{}",
                            requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getQueueOffset(), clientId);

                        responseHeader.setMinOffset(deFiBrokerController.getMessageStore().getMinOffsetInQueue(requestHeader.getTopic(), requestHeader.getQueueId()));
                        responseHeader.setMaxOffset(deFiBrokerController.getMessageStore().getMaxOffsetInQueue(requestHeader.getTopic(), requestHeader.getQueueId()));
                        responseHeader.setNextBeginOffset(requestHeader.getQueueOffset());
                        responseHeader.setSuggestWhichBrokerId(MixAll.MASTER_ID);
                        response.setCode(ResponseCode.PULL_NOT_FOUND);
                        response.setRemark("mq is locked by other client.");
                        return response;
                    }
                    //分到一个Q之后，更新offset为最新的ackOffset，避免消息重复
                    if (acquired) {
                        long nextBeginOffset = correctRequestOffset(group, topic, queueId, requestHeader.getQueueOffset());
                        if (nextBeginOffset != requestHeader.getQueueOffset().longValue()) {
                            RemotingCommand response = RemotingCommand.createResponseCommand(PullMessageResponseHeader.class);
                            final PullMessageResponseHeader responseHeader = (PullMessageResponseHeader) response.readCustomHeader();
                            response.setOpaque(request.getOpaque());
                            responseHeader.setMinOffset(deFiBrokerController.getMessageStore().getMinOffsetInQueue(requestHeader.getTopic(), requestHeader.getQueueId()));
                            responseHeader.setMaxOffset(deFiBrokerController.getMessageStore().getMaxOffsetInQueue(requestHeader.getTopic(), requestHeader.getQueueId()));
                            responseHeader.setNextBeginOffset(nextBeginOffset);
                            responseHeader.setSuggestWhichBrokerId(MixAll.MASTER_ID);
                            response.setCode(ResponseCode.PULL_NOT_FOUND);
                            response.setRemark("lock a queue success, update pull offset.");
                            LOG.info("update pull offset from [{}] to [{}] after client acquire a queue. clientId:{}, queueId:{}, topic:{}, group:{}",
                                requestHeader.getQueueOffset(), nextBeginOffset, clientId, requestHeader.getQueueId(),
                                requestHeader.getTopic(), requestHeader.getConsumerGroup());
                            return response;
                        }
                        else {
                            LOG.info("no need to update pull offset. clientId:{}, queueId:{}, topic:{}, group:{}, request offset: {}",
                                clientId, requestHeader.getQueueId(), requestHeader.getTopic(), requestHeader.getConsumerGroup(), requestHeader.getQueueOffset());
                        }
                    }
                }
            }
        }

        RemotingCommand response = super.processRequest(ctx, request);
        if (consumerGroupInfo != null) {
            ClientChannelInfo clientChannelInfo = consumerGroupInfo.getChannelInfoTable().get(ctx.channel());
            if (clientChannelInfo != null) {
                String clientId = clientChannelInfo.getClientId();
                deFiBrokerController.getClientRebalanceResultManager().updateListenMap(requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId(), clientId);
                clientChannelInfo.setLastUpdateTimestamp(System.currentTimeMillis());
            }
        }
        handleProcessResult(requestHeader, response);
        return response;
    }

    private void handleProcessResult(final PullMessageRequestHeader requestHeader, final RemotingCommand response) {
        if (response != null) {
            switch (response.getCode()) {
                case SUBSCRIPTION_GROUP_NOT_EXIST:
                case NO_PERMISSION:
                case SUBSCRIPTION_NOT_EXIST:
                case SUBSCRIPTION_NOT_LATEST:
                    response.setCode(PULL_NOT_FOUND);
                    final PullMessageResponseHeader responseHeader = (PullMessageResponseHeader) response.readCustomHeader();
                    responseHeader.setMinOffset(this.deFiBrokerController.getMessageStore().getMinOffsetInQueue(requestHeader.getTopic(), requestHeader.getQueueId()));
                    responseHeader.setMaxOffset(this.deFiBrokerController.getMessageStore().getMaxOffsetInQueue(requestHeader.getTopic(), requestHeader.getQueueId()));
                    responseHeader.setNextBeginOffset(requestHeader.getQueueOffset());
                    responseHeader.setSuggestWhichBrokerId(MixAll.MASTER_ID);
                    break;
            }
        }
    }

    private long correctRequestOffset(String group, String topic, int queueId, long requestOffset){
        long nextBeginOffset = deFiBrokerController.getConsumeQueueManager().queryOffset(group, topic, queueId);
        if (nextBeginOffset < 0 || nextBeginOffset < requestOffset){
            //1.查出来的ackOffset<0时不修改客户端请求的Offset，避免全新的Q开始消费时把Offset改为-1，造成offset_illegal；
            //2.ackOffset<请求的Offset时，不修改请求Offset，避免在主备模式下，主挂掉后切到备消费，备上同步的消费进度慢，导致客户端Offset往小调，造成重复；
            //这个发生在主挂掉后，客户端从备消费的场景下
            LOG.info("correct request offset. queryAckOffset = {} requestOffset = {} set nextBeginOffst to {}", nextBeginOffset, requestOffset, requestOffset);
            nextBeginOffset = requestOffset;
        }
        return nextBeginOffset;
    }
}
