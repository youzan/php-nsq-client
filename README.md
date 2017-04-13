# NSQ PHP CLIENT

## Reference
namespace: `Kdt\Iron\Queue\Queue`

### `class Queue`
#### `function push(string $topic, mixed $message) : bool`
发布单个消息

`$message` 为 `Kdt\Iron\Queue\Message` 对象 或简单类型，如 `string`, `array`，使用 `Message` 对象时可以使用一些高级属性

#### `function bulkPush(string $topic, array $messages) : bool`
批量发布消息

`$messages` 是一个一维消息数组

#### `function lastPushError() : string`
获取上次发布的错误信息

#### `function pop(mixed $topic, callable $callback, array $options) : mixed`
订阅消息。正常退出时会返回 false; 异常退出时返回 string，内容为异常信息

`$topic` 为 `string` 时，使用默认的 channel 名 `default`；指定 channel 时需要传入一个包含 2 个元素的一维数组 `[$topic_name, $channel]`

`$callback` 为 `function($message)`，有新消息时会触发回调，参数为 `Message` 对象。

`$options` 是一个关系数组，有以下选项：

- auto_delete : bool

  是否启用自动 ACK，默认 false

- keep_seconds : int

  daemon运行时间，单位秒，默认 900

- max_retry : int

  异常时的重试次数，默认 3

- retry_delay : int

  重试的间隔时间，单位秒，默认 5

- sub_ordered : bool
  
  是否启用顺序消费，默认 false

- sub_partition : int

  订阅指定的分区（顺序消费时），默认订阅所有分区或 nsq 节点

#### `function exitPop()`
退出 pop 后不会再接收新消息

#### `function delete(string $messageId) : bool`
删除消息（标记 ACK）

`$messageId` 通过 `Message` 对象的 `getId()` 方法获取 

#### `function later(int $seconds)`
消息延迟

单位秒，默认情况下最大 3600；
实现机制为抛出一个特定的异常对象 (nsqphp\Exception\RequeueMessageException) 给 SDK，注意外层尽量不要自己包装 try catch

#### `function retry()`
消息重试

#### `function close()`
主动关闭网络链接，只能在 pop 中使用



### `class Message`
#### `function getId() : string`
消息 ID

#### `function getTimestamp() : int`
消息产生时间戳

#### `function getAttempts() : int`
消息已经重试的次数，包含客户端主动重试和其他未知原因导致的服务端自动重试

### `function getPayload() : mixed`
消息内容

#### `function getTraceID() : int`
消息追踪 ID，由生产端传入

#### `function getShardingProof() : int`
分区依据
	 	 
#### `function setTraceID($id) : self`
设置消息追踪 ID，会传递到消费端

#### `function setShardingProof(int $sample) : self`
设置分区依据，会根据取模结果来决定目标分区

