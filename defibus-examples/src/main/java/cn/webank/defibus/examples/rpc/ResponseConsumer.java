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

package cn.webank.defibus.examples.rpc;

import java.util.List;

import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.webank.defibus.client.common.DeFiBusClientConfig;
import cn.webank.defibus.client.common.DeFiBusClientUtil;
import cn.webank.defibus.common.DeFiBusConstant;
import cn.webank.defibus.consumer.DeFiBusMessageListenerConcurrently;
import cn.webank.defibus.consumer.DeFiBusPushConsumer;
import cn.webank.defibus.producer.DeFiBusProducer;

public class ResponseConsumer {
    private static final Logger logger = LoggerFactory.getLogger(ResponseConsumer.class);

    public static void main(String[] args) throws MQClientException {
        String topic = "RequestTopic";
        DeFiBusClientConfig deFiBusClientConfig = new DeFiBusClientConfig();
        deFiBusClientConfig.setConsumerGroup("Your-group-name");
        deFiBusClientConfig.setPullBatchSize(32);
        deFiBusClientConfig.setThreadPoolCoreSize(12);
        deFiBusClientConfig.setClusterPrefix("XL");
        DeFiBusProducer deFiBusProducer = new DeFiBusProducer(deFiBusClientConfig);
        deFiBusProducer.setNamesrvAddr("127.0.0.1:9876");
        deFiBusProducer.start();
        DeFiBusPushConsumer deFiBusPushConsumer = new DeFiBusPushConsumer(deFiBusClientConfig);
        deFiBusPushConsumer.setNamesrvAddr("127.0.0.1:9876");
        deFiBusPushConsumer.registerMessageListener(new DeFiBusMessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus handleMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    String uniqueId = msg.getUserProperty(DeFiBusConstant.PROPERTY_RR_REQUEST_ID);
                    if (uniqueId == null) {
                        logger.info("REQUEST_ID is null from the request msg, will not reply this constant...");
                    } else {
                        try {
                            logger.info("begin handle: " + msg.toString());
                            Message replyMsg = DeFiBusClientUtil.createReplyMessage(msg, ("I am replying content").getBytes());
                            deFiBusProducer.reply(replyMsg, new SendCallback() {
                                @Override
                                public void onSuccess(SendResult sendResult) {
                                    logger.info("reply success. {}", msg.toString());
                                }
                                @Override
                                public void onException(Throwable e) {
                                    logger.info("reply fail. {}", msg.toString(), e);
                                }
                            });
                        } catch (InterruptedException | RemotingException | MQClientException | MQBrokerException e) {
                            logger.warn("{}", e);
                        }
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        deFiBusPushConsumer.subscribe(topic);
        deFiBusPushConsumer.start();

        //shutdown the consumer when application exits.
        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                deFiBusPushConsumer.shutdown();
            }
        });
    }
}
