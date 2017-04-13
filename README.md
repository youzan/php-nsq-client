# NSQ PHP CLIENT

## Reference
namespace: `Kdt\Iron\Queue\Queue`

### class `Queue`
#### `push(string $topic, mixed $message) : bool`

#### `bulkPush(string $topic, array $messages) : bool`

#### `lastPushError() : string`

#### `pop(mixed $topic, callable $callback, array $options) : mixed

#### `exitPop()`

#### `delete(string $messageId) : bool`

#### `later(int $seconds)`

#### `retry()`

#### `close()`

### class `Message`


