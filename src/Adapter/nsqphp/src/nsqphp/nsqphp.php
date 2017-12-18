<?php

namespace nsqphp;

use nsqphp\Connection\HTTP;
use nsqphp\Connection\Proxy;
use nsqphp\Exception\ConnectionFilterException;
use nsqphp\Exception\FailedOnAllNodesException;
use nsqphp\Exception\FailedOnNotLeaderException;
use nsqphp\Exception\PublishException;
use nsqphp\Exception\TopicNotExistException;
use React\EventLoop\LoopInterface;
use React\EventLoop\Factory as ELFactory;

use nsqphp\Logger\LoggerInterface;
use nsqphp\Lookup\LookupInterface;
use nsqphp\Connection\ConnectionInterface;
use nsqphp\Connection\ConnectionPool;
use nsqphp\Dedupe\DedupeInterface;
use nsqphp\RequeueStrategy\RequeueStrategyInterface;
use nsqphp\Message\MessageInterface;
use nsqphp\Message\Message;

class nsqphp
{
    /**
     * VERSION
     */
    const VERSION = '1.5.0';

    /**
     * Publish "consistency levels" [ish]
     */
    const PUB_ONE = 1;
    const PUB_TWO = 2;
    const PUB_QUORUM = 5;

    /**
     * nsqlookupd service
     * 
     * @var LookupInterface|NULL
     */
    private $nsLookup;

    /**
     * With proxy
     *
     * @var Proxy
     */
    private $proxy;
    
    /**
     * Dedupe service
     * 
     * @var DedupeInterface|NULL
     */
    private $dedupe;
    
    /**
     * Requeue strategy
     * 
     * @var RequeueStrategyInterface|NULL
     */
    private $requeueStrategy;
    
    /**
     * Logger, if any enabled
     * 
     * @var LoggerInterface|NULL
     */
    private $logger;
    
    /**
     * Connection timeout - in seconds
     * 
     * @var float
     */
    private $connectionTimeout;
    
    /**
     * Read/write timeout - in seconds
     * 
     * @var float
     */
    private $readWriteTimeout;
    
    /**
     * Read wait timeout - in seconds
     * 
     * @var float
     */
    private $readWaitTimeout;

    /**
     * Connection pool for subscriptions
     * 
     * @var ConnectionPool | ConnectionInterface[]
     */
    private $subConnectionPool;

    /**
     * Connection pool for publishing
     * 
     * @var ConnectionPool | ConnectionInterface[]
     */
    private $pubConnectionPool;

    /**
     * Time after which the publishing connections will be recycled
     *
     * @var int|NULL
     */
    private $pubConnectionsRecycling;

    /**
     * Publish success criteria (how many nodes need to respond)
     * 
     * @var integer
     */
    private $pubSuccessCount;

    /**
     * Publish nodes recorded with topic
     *
     * @var array
     */
    private $pubNodesRecorded = [];

    /**
     * Publish msg nums for warning (expect small pack)
     *
     * @var int
     */
    private $pubBodyWarningNums = 0;

    /**
     * Publish msg size for warning (expect small pack)
     *
     * @var int
     */
    private $pubBodyWarningSize = 0;

    /**
     * Event loop
     * 
     * @var LoopInterface
     */
    private $loop;
    
    /**
     * Wire reader
     * 
     * @var Wire\Reader
     */
    private $reader;
    
    /**
     * Wire writer
     * 
     * @var Wire\Writer
     */
    private $writer;

    /**
     * Pending messages (for finish)
     * @var array
     */
    private $pendingMessages;

    /**
     * Constructor
     * 
     * @param LookupInterface|NULL $nsLookup Lookup service for hosts from topic (optional)
     *      NB: $nsLookup service _is_ required for subscription
     * @param DedupeInterface|NULL $dedupe Deduplication service (optional)
     * @param RequeueStrategyInterface|NULL $requeueStrategy Our strategy
     *      for dealing with failures whilst processing SUBbed messages via
     *      callback - if any (optional)
     * @param LoggerInterface|NULL $logger Logging service (optional)
     * @param integer $connectionTimeout
     * @param integer $readWriteTimeout
     * @param integer $readWaitTimeout
     * @param integer $pubConnectionsRecycling
     */
    public function __construct(
            LookupInterface $nsLookup = NULL,
            DedupeInterface $dedupe = NULL,
            RequeueStrategyInterface $requeueStrategy = NULL,
            LoggerInterface $logger = NULL,
            $connectionTimeout = 3,
            $readWriteTimeout = 3,
            $readWaitTimeout = 15,
            $pubConnectionsRecycling = 55
            )
    {
        $this->nsLookup = $nsLookup;
        $this->dedupe = $dedupe;
        $this->requeueStrategy = $requeueStrategy;
        $this->logger = $logger;
        
        $this->connectionTimeout = $connectionTimeout;
        $this->readWriteTimeout = $readWriteTimeout;
        $this->readWaitTimeout = $readWaitTimeout;
        $this->pubSuccessCount = 1;
        
        $this->pubConnectionsRecycling = $pubConnectionsRecycling;

        $this->reader = new Wire\Reader;
        $this->writer = new Wire\Writer;

        $this->pubConnectionPool = ConnectionPool::getInstance('pub');
        $this->subConnectionPool = ConnectionPool::getInstance('sub');
    }

    /**
     * Set the nsq lookup service
     *
     * @param LookupInterface $nsLookup
     */
    public function setNsLookup(LookupInterface $nsLookup)
    {
        $this->nsLookup = $nsLookup;
    }

    /**
     * Get the nsq lookup service
     *
     * @return LookupInterface
     */
    public function getNsLookup()
    {
        return $this->nsLookup;
    }

    /**
     * @param Proxy $proxy
     */
    public function setProxy(Proxy $proxy)
    {
        $this->proxy = $proxy;
    }

    /**
     * @param $nums
     * @param $size
     */
    public function setPubBodyWarning($nums, $size)
    {
        $this->pubBodyWarningNums = $nums;
        $this->pubBodyWarningSize = $size;
    }

    /**
     * Set requeue strategy
     *
     * @param \nsqphp\RequeueStrategy\RequeueStrategyInterface $requeueStrategy
     */
    public function setRequeueStrategy(RequeueStrategyInterface $requeueStrategy = NULL)
    {
        $this->requeueStrategy = $requeueStrategy;
    }
    
    /**
     * Destructor
     */
    public function __destruct()
    {
        try
        {
            if ($this->subConnectionPool) {
                // say goodbye to each connection [sub]
                foreach ($this->subConnectionPool as $connection) {
                    if (!$connection->connected()) continue;
                    $connection->write($this->writer->close());
                    $connection->close();
                    if ($this->logger) {
                        $this->logger->debug(sprintf('nsqphp[sub] closing [%s]', (string)$connection));
                    }
                }
            }
            if ($this->pubConnectionPool) {
                // say goodbye to each connection [pub]
                foreach ($this->pubConnectionPool as $connection) {
                    if (!$connection->connected()) continue;
                    $connection->close();
                    if ($this->logger) {
                        $this->logger->debug(sprintf('nsqphp[pub] closing [%s]', (string)$connection));
                    }
                }
            }
        }
        catch (\Exception $e)
        {
            // logging
            if ($this->logger)
            {
                $this->logger->info(sprintf('#destruct# has exception : %s', $e->getMessage()));
            }
        }
    }
    
    /**
     * Define nsqd hosts to publish to
     * 
     * We'll remember these hosts for any subsequent publish() call, so you
     * only need to call this once to publish 
     *
     * @param string $topic
     * @param string|array $hosts
     * @param string $type connection type (tcp or http)
     * @param integer|NULL $cl Consistency level - basically how many `nsqd`
     *      nodes we need to respond to consider a publish successful
     *      The default value is nsqphp::PUB_ONE
     * 
     * @throws \InvalidArgumentException If bad CL provided
     * @throws \InvalidArgumentException If we cannot achieve the desired CL
     *      (eg: if you ask for PUB_TWO but only supply one node)
     * 
     * @return self This instance for call chaining
     */
    public function publishTo($topic, $hosts, $type = 'tcp', $cl = NULL)
    {
        // record custom hosts
        $this->pubNodesRecorded[$topic] = $hosts;
        // init connections (blocking)
        $this->initConnectionPool($this->pubConnectionPool, $hosts, $type, FALSE, $this->pubConnectionsRecycling);

        // work out success count
        if ($cl === NULL) {
            $cl = self::PUB_ONE;
        }
        switch ($cl) {
            case self::PUB_ONE:
            case self::PUB_TWO:
                $this->pubSuccessCount = $cl;
                break;
            case self::PUB_QUORUM:
                $this->pubSuccessCount = ceil($this->pubConnectionPool->count() / 2) + 1;
                break;
            default:
                throw new \InvalidArgumentException('Invalid consistency level');
                break;
        }
        if ($this->pubSuccessCount > $this->pubConnectionPool->count()) {
            throw new \InvalidArgumentException(sprintf('Cannot achieve desired consistency level with %s nodes', $this->pubConnectionPool->count()));
        }

        return $this;
    }
    
    /**
     * Publish message
     *
     * @param string $topic A valid topic name: [.a-zA-Z0-9_-] and 1 < length < 32
     * @param mixed $messageBag
     *
     * @throws Exception\PublishException If we don't get "OK" back from server
     *      (for the specified number of hosts - as directed by `publishTo`)
     *
     * @return array
     * @throws \Exception|FailedOnNotLeaderException|TopicNotExistException
     */
    public function publish($topic, $messageBag)
    {
        $filteredNodes = [];

        // check msg sharding
        if (is_object($messageBag) && $messageBag instanceof MessageInterface)
        {
            $limitedNode = $messageBag->getLimitedNode();
            if ($limitedNode)
            {
                $filteredNodes = $this->pubConnectionPool->filterConnections([$limitedNode]);
            }
        }

        if (empty($filteredNodes))
        {
            if (isset($this->pubNodesRecorded[$topic]))
            {
                $filteredNodes = $this->pubConnectionPool->filterConnections($this->pubNodesRecorded[$topic]);
            }
            else
            {
                throw new PublishException('Unconfirmed nodes');
            }
        }

        if (empty($filteredNodes))
        {
            throw new ConnectionFilterException('Target conn not found');
        }

        $success = 0;
        $errors = array();

        shuffle($filteredNodes);
        foreach ($filteredNodes as $conn) {
            /** @var $conn ConnectionInterface */
            try {
                switch ($conn->getConnType())
                {
                    case 'tcp':
                        list($iSuccess, $iErrors) = $this->publishViaTcp($conn, $topic, $messageBag);
                        break;
                    case 'http':
                        list($iSuccess, $iErrors) = $this->publishViaHttp($conn, $topic, $messageBag);
                        break;
                    default:
                        $iSuccess = 0;
                        $iErrors = [];
                }
                $success += $iSuccess;
                $errors = array_merge($errors, $iErrors);
                // logging
                if ($errors && $this->logger)
                {
                    $this->logger->info(sprintf('#nsqd-trying# exception-resolve [NSQ-ER|%s]', $this->tryFuncSrcFlag($topic, $conn)));
                }
            } catch (\Exception $e) {
                // logging
                if ($this->logger)
                {
                    $this->logger->info(sprintf('#nsqd-trying# exception-found [NSQ-EF|%s] ~ %s', $this->tryFuncSrcFlag($topic, $conn), $e->getMessage()));
                }
                // check is cluster-exception
                if ($this->checkIsClusterException($e))
                {
                    throw $e;
                }
                $errors[] = $e->getMessage();
            }
            if ($success >= $this->pubSuccessCount) {
                break;
            }
        }

        if ($success < $this->pubSuccessCount)
        {
            throw new FailedOnAllNodesException(end($errors));
        }

//        if ($success < $this->pubSuccessCount) {
//            throw new Exception\PublishException(
//                    sprintf('Failed to publish message; required %s for success, achieved %s. Errors were: %s', $this->pubSuccessCount, $success, implode(', ', $errors))
//                    );
//        }

        return [
            'success' => $success,
            'errors' => $errors
        ];
    }

    /**
     * @param ConnectionInterface $conn
     * @param $topic
     * @param $messageBag
     * @return array
     * @throws FailedOnNotLeaderException|TopicNotExistException
     */
    private function publishViaTcp(ConnectionInterface $conn, $topic, $messageBag)
    {
        $success = 0;
        $errors = [];
        if ($messageBag instanceof MessageInterface)
        {
            $mBody = $this->writer->publish($topic, $messageBag, $conn->getPartitionID());
            $this->checkPubMsgSize(strlen($mBody), $topic);
        }
        else
        {
            // 不再使用 mpub，因为 mpub 不支持扩展字段，无法支持service chain
            foreach ($messageBag as $messageItem)
            {
                $result = $this->publishViaTcp($conn, $topic, $messageItem);
                $success+= $result[0];
                $errors = array_merge($errors, $result[1]);
            }
            return [
                $success,
                $errors
            ];
            // $messages = [];
            // foreach ($messageBag as $messageItem)
            // {
            //     if ($messageItem instanceof MessageInterface)
            //     {
            //         $cBody = $messageItem->getPayload();
            //         $this->checkPubMsgSize(strlen($cBody), $topic);
            //         $messages[] = $cBody;
            //     }
            // }
            // $this->checkPubMsgNums(count($messages), $topic);
            // $mBody = $this->writer->multiPublish($topic, $messages, $conn->getPartitionID());
        }

        $conn->write($mBody);

        $frame = $this->reader->readFrame($conn);

        while ($this->reader->frameIsHeartbeat($frame)) {
            $conn->write($this->writer->nop());
            $frame = $this->reader->readFrame($conn);
        }

        if ($this->reader->frameIsResponse($frame, 'OK')) {
            $success++;
        } else if ($this->reader->frameIsError($frame) && null !== $e = $this->getClusterException($frame['error'])) {
            throw $e;
        } else {
            $errors[] = $frame['error'];
        }

        return [
            $success,
            $errors
        ];
    }

    /**
     * @param ConnectionInterface $conn
     * @param $topic
     * @param $messageBag
     * @return array
     * @throws FailedOnNotLeaderException|TopicNotExistException
     */
    private function publishViaHttp(ConnectionInterface $conn, $topic, $messageBag)
    {
        $success = 0;
        $errors = [];

        if ($messageBag instanceof MessageInterface)
        {
            list($url, $data) = $this->writer->publishForHttp($topic, $messageBag, $conn->getPartitionID());
            $this->checkPubMsgSize(strlen($data), $topic);
        }
        else
        {
            $messages = [];
            foreach ($messageBag as $messageItem)
            {
                if ($messageItem instanceof MessageInterface)
                {
                    $cBody = $messageItem->getPayload();
                    $this->checkPubMsgSize(strlen($cBody), $topic);
                    $messages[] = $cBody;
                }
            }
            $this->checkPubMsgNums(count($messages), $topic);
            list($url, $data) = $this->writer->multiPublishForHttp($topic, $messages);
        }

        $result = $conn->post($url, $data);
        if ($result === 'OK')
        {
            $success ++;
        }
        else
        {
            $json = json_decode($result, true);
            if (is_array($json) && (isset($json['status']) || isset($json['status_code'])))
            {
                if ($json['status'] == 'OK')
                {
                    $success ++;
                }
                else if ($json['status_code'] == 200)
                {
                    $success ++;
                }
                else
                {
                    $errInfo = isset($json['status_txt']) ? $json['status_txt'] : 'some error';
                    if (null !== $e = $this->getClusterException($errInfo)) {
                        throw $e;
                    } else {
                        $errors[] = $errInfo;
                    }
                }
            }
            else
            {
                $errors[] = $result;
            }
        }

        return [
            $success,
            $errors
        ];
    }

    /**
     * @param $error
     * @return FailedOnNotLeaderException|TopicNotExistException|null
     */
    private function getClusterException($error)
    {
        if (is_numeric(strpos($error, 'E_TOPIC_NOT_EXIST')))
        {
            return new TopicNotExistException;
        }
        else if (is_numeric(strpos($error, 'E_FAILED_ON_NOT_LEADER')))
        {
            return new FailedOnNotLeaderException;
        }
        else
        {
            return null;
        }
    }

    /**
     * @param $topic
     * @param ConnectionInterface $conn
     * @return string
     */
    private function tryFuncSrcFlag($topic, ConnectionInterface $conn)
    {
        return 'T='.$topic.',C='.strtoupper($conn->getConnType()).',H='.(string)$conn.',P='.($conn->getProxy()->isEnabled() ? 'ON' : 'OFF');
    }
    
    /**
     * Subscribe to topic/channel
     *
     * @param array $hosts Host list for subscribe to
     * @param string $topic A valid topic name: [.a-zA-Z0-9_-] and 1 < length < 32
     * @param string $channel Our channel name: [.a-zA-Z0-9_-] and 1 < length < 32
     *      "In practice, a channel maps to a downstream service consuming a topic."
     * @param callable $callback A callback that will be executed with a single
     *      parameter of the message object dequeued. Simply return TRUE to 
     *      mark the message as finished or throw an exception to cause a
     *      backed-off requeue
     * @param array $params
     * bool auto_delete // Is automatic delete message from queue (means set msg FIN and client RDY)
     * bool sub_ordered // Is subscribe in ordered state
     * int  msg_timeout // Message timeout. null means use default
     * string tag // desired tag, can be null
     * array ext_filter
     * 
     * @throws \RuntimeException If we don't have a valid callback
     * @throws \InvalidArgumentException If we don't have a valid callback
     * 
     * @return nsqphp This instance of call chaining
     */
    public function subscribe($hosts, $topic, $channel, $callback, $params)
    {
        $autoDelete = isset($params['auto_delete']) ? boolval($params['auto_delete']) : true;
        $subOrdered = isset($params['sub_ordered']) ? boolval($params['sub_ordered']) : false;
        $msgTimeout = isset($params['msg_timeout']) ? intval($params['msg_timeout']) : null;
        $tag = isset($params['tag']) ? strval($params['tag']) : null;
        $extFilter = isset($params['ext_filter']) ? $params['ext_filter'] : null;
        
        if (!is_callable($callback)) {
            throw new \InvalidArgumentException(
                '"callback" invalid; expecting a PHP callable'
            );
        }
        // we need to instantiate a new connection for every nsqd that we need
        // to fetch messages from for this topic/channel

        /*
        $hosts = $this->getNsLookup()->lookupHosts($topic, 'sub');
        */

        if (empty($hosts)) {
            throw new \RuntimeException(
                'Found none hosts for topic "'.$topic.'", exiting.'
            );
        } else {
            // clear status
            $this->closeLoop();
            $this->closeConnection($this->subConnectionPool);

            // empty exists connections
            $this->subConnectionPool->emptyConnections();

            // init connections (non-blocking)
            $this->initConnectionPool($this->subConnectionPool, $hosts, 'tcp', TRUE, NULL);
        }

        if ($this->logger) {
            $hostIps = [];
            array_walk($hosts, function ($host) use (&$hostIps) {
                $hostIps[] = $host['host'];
            });
            $this->logger->debug("Found the following hosts for topic \"$topic\": " . implode(',', $hostIps));
        }

        foreach ($this->subConnectionPool as $conn)
        {
            $nsq = $this;
            if ($conn->isYzCluster())
            {
                //$conn->setHasExtendData(true); 
                if ($subOrdered)
                {
                    $conn->setOrderedSub();
                }
                if (!empty($tag))
                {
                    $conn->setDesiredTag($tag);
                }
                if (!empty($extFilTer))
                {
                    $conn->setExtFilter($extFilter);
                }
            }
            // looping
            $this->initLoop()->addReadStream
            (
                $conn->getSocket(),
                function ($socket) use ($nsq, $callback, $topic, $channel, $autoDelete)
                {
                    $nsq->readAndDispatchMessage($socket, $topic, $channel, $callback, $autoDelete);
                }
            );

            // identify
            $identifyData = [
                'client_id' => (string)getmypid(), 
                'hostname' => gethostname(), 
                'user_agent' => sprintf('php-iron/%s', self::VERSION),
            ];
            $msgTimeout = intval($msgTimeout);
            if ($msgTimeout > 0)
            {
                $identifyData['msg_timeout'] = $msgTimeout;
            }
            if ($conn->getHasExtendData())
            {
                $identifyData['extend_support'] = true;
                if (!empty($tag))
                {
                    $identifyData['desired_tag'] = trim($tag);
                }
                if (!empty($extFilter))
                {
                    $identifyData['ext_filter'] = $extFilter;
                }
            }
            else
            {
                $identifyData['extend_support'] = false;
            }
            $conn->write($this->writer->identify($identifyData));

            // subscribe
            $conn->write($this->writer->subscribe($topic, $channel, $conn->getPartitionID(), $conn->isOrderedSub()));
            $conn->write($this->writer->ready(1));
        }
        return $this;
    }

    /**
     * Run subscribe event loop
     *
     * @param int $timeout (default=0) timeout in seconds
     */
    public function run($timeout = 0)
    {
        if ($timeout > 0) {
            $that = $this;
            $this->initLoop()->addTimer($timeout, function () use ($that) {
                $that->stop();
            });
        }
        $this->initLoop()->run();
    }

    /**
     * Stop subscribe event loop
     */
    public function stop()
    {
        $this->initLoop()->stop();
    }

    /**
     * Add timer loop
     * @param int $timeout
     * @param $callback
     */
    public function timer($timeout = 1, $callback)
    {
        $loop = $this->initLoop();
        $loop->addPeriodicTimer($timeout, function() use($callback, $loop) {
            call_user_func_array($callback, [$loop]);
        });
        $loop->run();
    }

    /**
     * Read/dispatch callback for async sub loop
     *
     * @param Resource $socket The socket that a message is available on
     * @param string $topic The topic subscribed to that yielded this message
     * @param string $channel The channel subscribed to that yielded this message
     * @param callable $callback The callback to execute to process this message
     * @param bool $autoDelete
     */
    public function readAndDispatchMessage($socket, $topic, $channel, $callback, $autoDelete = true)
    {
        $connection = $this->subConnectionPool->findConnection($socket);

        $frame = $this->reader->readFrame($connection);

        if ($this->logger) {
            $this->logger->debug(sprintf('Read frame for topic=%s channel=%s [%s] %s', $topic, $channel, (string)$connection, json_encode($frame)));
        }

        // intercept errors/responses
        if ($this->reader->frameIsHeartbeat($frame)) {
            if ($this->logger) {
                $this->logger->debug(sprintf('HEARTBEAT [%s]', (string)$connection));
            }
            $connection->write($this->writer->nop());
        } elseif ($this->reader->frameIsMessage($frame)) {
            $msg = Message::fromFrame($frame);
            
            if ($this->dedupe !== NULL && $this->dedupe->containsAndAdd($topic, $channel, $msg)) {
                if ($this->logger) {
                    $this->logger->debug(sprintf('Deduplicating [%s] "%s"', (string)$connection, $msg->getId()));
                }
            } else {
                $desiredTag = $connection->getDesiredTag();
                if (!empty($desiredTag)) {
                    if ($msg->getTag() !== $desiredTag) {
                        $autoDelete = false;
                        if ($this->logger) {
                            $this->logger->error(sprintf('Received a message[%s] which tag not match: "%s" != "%s"',
                                $msg->getId(), $msg->getTag(), $desiredTag));
                        }
                        return;
                    }
                }
                $this->pendingMessages[$msg->getId()] = ['connection' => $connection, 'raw_id' => $msg->getRawId()];
                $filtered = $this->isFiltered($msg, $connection);
                echo "FILTERED: "; var_dump($filtered);
                try {
                    if (!$filtered) {
                        call_user_func($callback, $msg);
                    }
                } catch (Exception\ExpiredMessageException $e) {
                    // expired message
                    if ($this->logger) {
                        $this->logger->info(sprintf(
                            'Expired message [%s] "%s": (%s)%s:%s: %s',
                            (string)$connection,
                            $msg->getId(),
                            get_class($e),
                            $e->getFile(),
                            $e->getLine(),
                            $e->getMessage()
                        ));
                    }
                } catch (\Exception $e) {
                    // erase knowledge of this msg from dedupe
                    if ($this->dedupe !== NULL) {
                        $this->dedupe->erase($topic, $channel, $msg);
                    }
                    
                    if ($this->logger) {
                        $logMsg = sprintf(
                            '#message# ['.$topic.':'.$channel.'] Error processing [%s] "%s": (%s)%s:%s: %s',
                            (string)$connection,
                            $msg->getId(),
                            get_class($e),
                            $e->getFile(),
                            $e->getLine(),
                            $e->getMessage()
                        );
                        $e instanceof Exception\RequeueMessageException ? $this->logger->debug($logMsg) : $this->logger->warn($logMsg);
                    }

                    $requeue = false;
                    $requeueDelay = 0;
                    // explicit requeuing
                    if ($e instanceof Exception\RequeueMessageException) {
                        $requeue = true;
                        $requeueDelay = $e->getDelay();
                    }
                    // requeue message according to backoff strategy; continue
                    else if ($this->requeueStrategy !== NULL
                            && ($requeueDelay = $this->requeueStrategy->shouldRequeue($msg)) !== NULL) {
                        $requeue = true;
                    }
                    if ($requeue) {
                        // requeue
                        if ($this->logger) {
                            $this->logger->debug(sprintf('Requeuing [%s] "%s" with delay "%s"', (string)$connection, $msg->getId(), $requeueDelay));
                        }
                        $connection->write($this->writer->requeue($msg->getRawId(), $requeueDelay));
                        return;
                    }
                    if ($this->logger) {
                        $this->logger->debug(sprintf('Not requeuing [%s] "%s"', (string)$connection, $msg->getId()));
                    }
                }
            }

            if ($autoDelete || $filtered) {
                // mark as done; get next on the way
                $this->deleteMessage($msg->getId());
            }

        } elseif ($this->reader->frameIsOk($frame)) {
            if ($this->logger) {
                $this->logger->debug(sprintf('Ignoring "OK" frame in SUB loop'));
            }
        } else if ($this->reader->frameIsError($frame)) {
            if ($this->logger) {
                $this->logger->error(sprintf('#poping# Error frame received: %s', $frame['error']));
            }
            if (null !== $e = $this->getClusterException($frame['error'])) {
                throw $e;
            }
        } else if ($this->reader->frameIsBroken($frame)) {
            if ($this->logger) {
                $this->logger->error(sprintf('#poping# Broken frame received: %s', json_encode($frame)));
            }
        } else {
            // @todo handle error responses a bit more cleverly
            throw new Exception\ProtocolException("Unexpected frame received: " . json_encode($frame));
        }
    }

    private function isFiltered($msg, $connection)
    {
        $extFilter = $connection->getExtFilter();
        if (empty($extFilter)) {
            return false;
        }
        $extends = $msg->getExtends();
        if (empty($extends)) {
            return false;
        }
        $key = $extFilter[0];
        $accept = isset($extends[$key]) ? $extends[$key] : null;
        if ($accept === null) {
            return false;
        }
        return $accept !== $extFilter[1];
    }
    
    /**
     * Remove message from queue
     * @param $messageId
     * @return bool
     */
    public function deleteMessage($messageId)
    {
        if (isset($this->pendingMessages[$messageId]))
        {
            $stack = $this->pendingMessages[$messageId];
            if (isset($stack['connection']))
            {
                $connection = $stack['connection'];
                if ($connection instanceof ConnectionInterface)
                {
                    $connection->write($this->writer->finish($stack['raw_id']));
                    unset($this->pendingMessages[$messageId]);
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Connection(all) close
     */
    public function close()
    {
        $this->closeConnection($this->pubConnectionPool);
        $this->closeConnection($this->subConnectionPool);
    }

    /**
     * Connection close
     * @param ConnectionPool $pool
     */
    private function closeConnection(&$pool)
    {
        if ($pool instanceof ConnectionPool)
        {
            foreach ($pool as $connection)
            {
                if ($connection instanceof ConnectionInterface)
                {
                    $connection->close();
                    if ($this->logger) {
                        $this->logger->debug(sprintf('nsqphp[manual] closing [%s]', (string)$connection));
                    }
                }
            }
            unset($pool);
        }
    }

    /**
     * @param ConnectionPool $pool
     * @param $nodes
     * @param $connType
     * @param $blocking
     * @param $connectionRecycling
     * @return ConnectionPool
     */
    private function initConnectionPool(&$pool, $nodes, $connType, $blocking, $connectionRecycling)
    {
        foreach ($nodes as $node)
        {
            if (!$pool->hasHost($node['host'], $node['ports']['tcp'], $node['ports']['http'], $node['partition']))
            {
                $pool->add($this->createConnection($node, $connType, $blocking, $connectionRecycling));
            }
        }
        return $pool;
    }

    /**
     * @param $node
     * @param $type
     * @param $blocking
     * @param $connectionRecycling
     * @return Connection\Connection
     */
    private function createConnection($node, $type, $blocking, $connectionRecycling)
    {
        $conn = new Connection\Connection(
            $node['host'],
            $node['ports']['tcp'],
            $node['ports']['http'],
            $node['cluster'],
            $node['partition'],
            $type,
            $this->connectionTimeout,
            $this->readWriteTimeout,
            $this->readWaitTimeout,
            $blocking,
            array($this, 'connectionCallback'),
            $connectionRecycling
        );
        $conn->setProxy($this->proxy);
        if (isset($node['meta']['extend_support']))
        {
            $conn->setHasExtendData($node['meta']['extend_support']);
        }
        return $conn;
    }

    /**
     * Connection callback
     * 
     * @param ConnectionInterface $connection
     */
    public function connectionCallback(ConnectionInterface $connection)
    {
        if ($this->logger) {
            $this->logger->debug("Connecting to " . (string)$connection . " and saying hello");
        }
        if ($this->proxy->notEnabled())
        {
            $connection->write($this->writer->magic());
        }
    }

    /**
     * send nop command
     *
     * @param ConnectionInterface $conn
     */
    public function nop()
    {
        $pool = $this->subConnectionPool;
        if ($pool instanceof ConnectionPool)
        {
            foreach ($pool as $connection)
            {
                if ($connection instanceof ConnectionInterface)
                {
                    $connection->write($this->writer->nop());
                }
            }
            unset($pool);
        }
   }

    /**
     * Loop close
     */
    private function closeLoop()
    {
        if ($this->loop)
        {
            $this->loop->stop();
            $this->loop = null;
        }
    }

    /**
     * @return LoopInterface
     */
    private function initLoop()
    {
        if (is_null($this->loop))
        {
            $this->loop = ELFactory::create();
        }
        return $this->loop;
    }

    /**
     * @param $size
     * @param $topic
     */
    private function checkPubMsgSize($size, $topic)
    {
        if ($this->pubBodyWarningSize && $size > $this->pubBodyWarningSize)
        {
            if ($this->logger)
            {
                $this->logger->info(sprintf('#publish# Message body too large (%s, %d)', $topic, $size));
            }
        }
    }

    /**
     * @param $nums
     * @param $topic
     */
    private function checkPubMsgNums($nums, $topic)
    {
        if ($this->pubBodyWarningNums && $nums > $this->pubBodyWarningNums)
        {
            if ($this->logger)
            {
                $this->logger->info(sprintf('#publish# Message items too many (%s, %d)', $topic, $nums));
            }
        }
    }

    /**
     * @param \Exception $e
     * @return bool
     */
    private function checkIsClusterException(\Exception $e)
    {
        if ($e instanceof FailedOnNotLeaderException || $e instanceof TopicNotExistException)
        {
            return true;
        }
        else
        {
            return false;
        }
    }

    /**
     * Get nsqd node stats
     * @param $host
     * @return array
     */
    public function node_stats($host)
    {
        if (strpos($host, ':') === FALSE) {
            $host .= ':4151';
        }

        $timeout = [
            CURLOPT_CONNECTTIMEOUT => $this->connectionTimeout,
            CURLOPT_TIMEOUT        => $this->readWaitTimeout,
        ];

        $url = "http://{$host}/stats?format=json";

        list($error, $result) = HTTP::get($url, $timeout);

        if ($error)
        {
            return [];
        }

        $r = json_decode($result, TRUE);
        $topics = isset($r['data'], $r['data']['topics']) ? $r['data']['topics'] : array();
        if ($topics)
        {
            return $topics;
        }
        else
        {
            return [];
        }
    }
}
