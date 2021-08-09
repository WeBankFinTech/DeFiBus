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
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.broker.processor.PullMessageProcessor;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.PullMessageResponseHeader;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.webank.defibus.common.DeFiBusConstant.CLIENT_ID;
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
        RemotingCommand response = super.processRequest(ctx, request);

        final PullMessageRequestHeader requestHeader =
            (PullMessageRequestHeader) request.decodeCommandCustomHeader(PullMessageRequestHeader.class);
        ConsumerGroupInfo consumerGroupInfo = deFiBrokerController.getConsumerManager().getConsumerGroupInfo(requestHeader.getConsumerGroup());
        if (consumerGroupInfo != null) {
            ClientChannelInfo clientChannelInfo = consumerGroupInfo.getChannelInfoTable().get(ctx.channel());
            String clientId = request.getExtFields().getOrDefault(CLIENT_ID, null);
            if (clientChannelInfo != null) {
                if (StringUtils.isBlank(clientId) && clientChannelInfo.getClientId() != null) {
                    clientId = clientChannelInfo.getClientId();
                }
                if (clientId == null) {
                    LOG.warn("clientId is null, group {}, channel {}", consumerGroupInfo.getGroupName(), ctx.channel());
                }
                deFiBrokerController.getClientRebalanceResultManager().updateListenMap(requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId(), clientId);
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
}
