## 5.访问控制表
&nbsp;&nbsp;&nbsp;
当Consumer去注册、断开连接、注册等上下线发生时，客户端做负载均衡，负载均衡之后，queue的监听关系会发生变化，一个queue可能由ConsumerA消费变成ConsumerB消费，在这个过程中，由于ACK的时效和短时的重复监听，会出现消息重复的问题。有些应用对消息重复比较敏感，因此需要对消息消费进行控制，降低消息重复量。控制表功能在解决重复监听问题的思路是：在broker上记录每个queue是由哪一个Consumer消费，Broker对每个拉消息的请求进行校验，检查拉消息的客户端及所要拉的queue与控制表里记录的监听关系是否一致，如果一致，则允许拉消息。如果不一致，则拒绝此次请求。Broker通过控制表来限制每个queue在同一时间点只有一个客户端能够消费（除了广播），避免重复监听引起的重复消费。


---
#### Links:
* [架构介绍](../../../README.md)
* [Request-Reply调用](docs/cn/features/1-request-response-call.md)
* [灰度发布](docs/cn/features/2-dark-launch.md)
* [熔断机制](docs/cn/features/3-circuit-break-mechanism.md)
* [服务就近](docs/cn/features/4-invoke-service-nearby.md)
* [应用多活](docs/cn/features/5-multi-active.md)
* [动态扩缩队列](docs/cn/features/6-dynamic-adjust-queue.md)
* [容错机制](docs/cn/features/8-fault-tolerant.md)