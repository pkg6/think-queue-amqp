<?php

namespace think\queue\connector;


use Exception;
use LogicException;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPLazyConnection;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Exception\AMQPProtocolChannelException;
use PhpAmqpLib\Exchange\AMQPExchangeType;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use think\helper\Arr;
use think\helper\Str;
use think\queue\Connector;
use think\queue\InteractsWithTime;
use think\queue\job\Amqp as AmqpJob;



class AmqpConnector extends Connector
{
    use InteractsWithTime;

    /**
     * @var AMQPStreamConnection
     */
    protected $connection;
    /**
     * @var AMQPChannel
     */
    protected $channel;
    /**
     * @var string
     */
    protected $default;
    /**
     * @var array
     */
    protected $options;
    /**
     * @var array
     */
    protected $exchanges;
    /**
     * @var AMQPLazyConnection
     */
    protected $amqp;


    /**
     * Amqp constructor.
     * @param $config
     * @throws Exception
     */
    public function __construct($config)
    {
        $this->createConnection($config);
    }

    /**
     * @param $config
     * @throws Exception
     */
    public function createConnection($config)
    {
        if (!class_exists(AMQPLazyConnection::class)) {
            throw new LogicException('Please composer require php-amqplib/php-amqplib ^3.0');
        }
        $config['options']['heartbeat'] = 0;
        $this->default                  = $config['queue'] ?? 'default';
        $this->options                  = $config['options'];
        $this->amqp                     = AMQPLazyConnection::create_connection($config['hosts'], $config['options'] ?? []);
        $this->channel                  = $this->amqp->channel();
    }


    /**
     * @param null $queue
     * @return int
     */
    public function size($queue = null)
    {
        $queue = $this->getQueue($queue);
        if ($this->isQueueExists($queue) === false) {
            return 0;
        }
        // create a temporary channel, so the main channel will not be closed on exception
        $channel = $this->amqp->channel();
        [, $size] = $channel->queue_declare($queue, true);
        $channel->close();
        return $size;
    }


    /**
     * @param $job
     * @param string $data
     * @param null $queue
     * @return mixed
     * @throws AMQPProtocolChannelException
     */
    public function push($job, $data = '', $queue = null)
    {
        return $this->pushRaw($this->createPayload($job, $data), $queue, []);
    }


    /**
     * 生产消息.
     * @param $payload
     * @param null $queue
     * @param array $options
     * @return mixed
     * @throws AMQPProtocolChannelException
     */
    public function pushRaw($payload, $queue = null, array $options = [])
    {
        [$destination, $exchange, $exchangeType, $attempts] = $this->publishProperties($queue, $options);
        $this->declareDestination($destination, $exchange, $exchangeType);
        [$message, $correlationId] = $this->createMessage($payload, $attempts);
        $this->channel->basic_publish($message, $exchange, $destination, true, false);
        return $correlationId;
    }


    /**
     * @param $delay
     * @param $job
     * @param string $data
     * @param null $queue
     * @return mixed
     * @throws AMQPProtocolChannelException
     */
    public function later($delay, $job, $data = '', $queue = null)
    {
        return $this->laterRaw($delay, $this->createPayload($job, $data), $queue);
    }

    /**
     * 生产延迟消息.
     * @param $delay
     * @param $payload
     * @param null $queue
     * @param int $attempts
     * @return mixed
     * @throws AMQPProtocolChannelException
     *
     */
    public function laterRaw($delay, $payload, $queue = null, $attempts = 0)
    {
        //$delay s后超时
        $ttl = $this->secondsUntil($delay) * 1000;
        // When no ttl just publish a new message to the exchange or queue
        if ($ttl <= 0) {
            return $this->pushRaw($payload, $queue, ['delay' => $delay, 'attempts' => $attempts]);
        }
        $destination = $this->getQueue($queue) . '.delay.' . $ttl;
        //queue_declare
        $this->declareQueue($destination, true, false, $this->getDelayQueueArguments($this->getQueue($queue), $ttl));

        [$message, $correlationId] = $this->createMessage($payload, $attempts);
        // Publish directly on the delayQueue, no need to publish trough an exchange.
        $this->channel->basic_publish($message, null, $destination, true, false);

        return $correlationId;
    }


    /**
     * 消费消息.
     * @param null $queue
     * @return AmqpJob|null
     */
    public function pop($queue = null)
    {
        try {
            $queue = $this->getQueue($queue);
            if ($job = $this->channel->basic_get($queue)) {
                return new AmqpJob($this->app, $this, $this->amqp, $job, $queue);
            }
        } catch (AMQPProtocolChannelException $exception) {
            // If there is not exchange or queue AMQP will throw exception with code 404
            // We need to catch it and return null
            if ($exception->amqp_reply_code === 404) {
                // Because of the channel exception the channel was closed and removed.
                // We have to open a new channel. Because else the worker(s) are stuck in a loop, without processing.
                $this->channel = $this->amqp->channel();
                return null;
            }
        }

        return null;
    }


    /**
     * 删除消息.
     * @param AmqpJob $job
     */
    public function ack(amqpJob $job)
    {
        $this->channel->basic_ack($job->getMessage()->getDeliveryTag());
    }

    /**
     * @param $job
     * @param string $data
     * @return array
     */
    protected function createPayloadArray($job, $data = '')
    {
        return array_merge(parent::createPayloadArray($job, $data), [
            'id'       => $this->getRandomId(),
            'attempts' => 0,
        ]);
    }

    /**
     * Create a AMQP message.
     * @param $payload
     * @param int $attempts
     * @return array
     */
    protected function createMessage($payload, int $attempts = 0)
    {
        $properties     = [
            'content_type'  => 'application/json',
            'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
        ];
        $currentPayload = json_decode($payload, true, 512);
        if ($correlationId = $currentPayload['id'] ?? null) {
            $properties['correlation_id'] = $correlationId;
            /** aliyun message_id **/
            $properties['message_id'] = $correlationId;
        }
        if ($this->isPrioritizeDelayed()) {
            $properties['priority'] = $attempts;
        }
        if (isset($currentPayload['data']['command'])) {
            $commandData = unserialize($currentPayload['data']['command']);
            if (property_exists($commandData, 'priority')) {
                $properties['priority'] = $commandData->priority;
            }
        }
        $message = new AMQPMessage($payload, $properties);
        $message->set('application_headers', new AMQPTable([
            'thinkphp' => [
                'attempts' => $attempts,
            ],
        ]));
        return [$message, $correlationId];
    }

    /**
     * Declare the destination when necessary.
     * @param string $destination
     * @param string|null $exchange
     * @param string|null $exchangeType
     * @return void
     * @throws AMQPProtocolChannelException
     *
     */
    protected function declareDestination(string $destination, ?string $exchange = null, string $exchangeType = AMQPExchangeType::DIRECT)
    {
        // When a exchange is provided and no exchange is present in RabbitMQ, create an exchange.
        if ($exchange && !$this->isExchangeExists($exchange)) {
            $this->declareExchange($exchange, $exchangeType);
        }
        // When a exchange is provided, just return.
        if ($exchange) {
            return;
        }
        // When the queue already exists, just return.
        if ($this->isQueueExists($destination)) {
            return;
        }
        // Create a queue for amq.direct publishing.
        $this->declareQueue($destination, true, false, $this->getQueueArguments($destination));
    }

    /**
     * Declare a queue in rabbitMQ, when not already declared.
     * @param string $name
     * @param bool $durable
     * @param bool $autoDelete
     * @param array $arguments
     * @return void
     */
    protected function declareQueue(string $name, bool $durable = true, bool $autoDelete = false, array $arguments = [])
    {
        $this->channel->queue_declare(
            $name,
            false,
            $durable,
            false,
            $autoDelete,
            false,
            new AMQPTable($arguments)
        );
    }

    /**
     * Checks if the given exchange already present/defined in RabbitMQ.
     * Returns false when when the exchange is missing.
     * @param string $exchange
     * @return bool
     * @throws AMQPProtocolChannelException
     *
     */
    protected function isExchangeExists(string $exchange)
    {
        if ($this->isExchangeDeclared($exchange)) {
            return true;
        }
        try {
            // create a temporary channel, so the main channel will not be closed on exception
            $channel = $this->amqp->channel();
            $channel->exchange_declare($exchange, '', true);
            $channel->close();
            $this->exchanges[] = $exchange;
            return true;
        } catch (AMQPProtocolChannelException $exception) {
            if ($exception->amqp_reply_code === 404) {
                return false;
            }

            throw $exception;
        }
    }

    /**
     * Checks if the given queue already present/defined in RabbitMQ.
     * Returns false when when the queue is missing.
     * @param string|null $name
     * @return bool
     */
    protected function isQueueExists(string $name = null)
    {
        try {
            // create a temporary channel, so the main channel will not be closed on exception
            $channel = $this->amqp->channel();
            $channel->queue_declare($name, true);
            $channel->close();

            return true;
        } catch (AMQPProtocolChannelException $exception) {
            if ($exception->amqp_reply_code === 404) {
                return false;
            }

            return false;
        }
    }

    /**
     * Declare a exchange in rabbitMQ, when not already declared.
     * @param string $name
     * @param string $type
     * @param bool $durable
     * @param bool $autoDelete
     * @param array $arguments
     * @return void
     */
    protected function declareExchange(string $name, string $type = AMQPExchangeType::DIRECT, bool $durable = true, bool $autoDelete = false, array $arguments = [])
    {
        if ($this->isExchangeDeclared($name)) {
            return;
        }
        $this->channel->exchange_declare(
            $name,
            $type,
            false,
            $durable,
            $autoDelete,
            false,
            true,
            new AMQPTable($arguments)
        );
    }

    /**
     * Checks if the exchange was already declared.
     * @param string $name
     * @return bool
     */
    protected function isExchangeDeclared(string $name)
    {
        return in_array($name, $this->exchanges, true);
    }

    /**
     * Determine all publish properties.
     * @param $queue
     * @param array $options
     * @return array
     */
    protected function publishProperties($queue, array $options = [])
    {
        $queue        = $this->getQueue($queue);
        $attempts     = Arr::get($options, 'attempts', 0);
        $destination  = $this->getRoutingKey($queue);
        $exchange     = $this->getExchange(Arr::get($options, 'exchange'));
        $exchangeType = $this->getExchangeType(Arr::get($options, 'exchange_type'));
        return [$destination, $exchange, $exchangeType, $attempts];
    }

    /**
     * Get the routing-key for when you use exchanges
     * The default routing-key is the given destination.
     * @param string $destination
     * @return string
     */
    protected function getRoutingKey(string $destination)
    {
        return ltrim(sprintf(Arr::get($this->options, 'exchange_routing_key') ?: '%s', $destination), '.');
    }

    /**
     * Get the exchange name, or &null; as default value.
     * @param string|null $exchange
     * @return string|null
     */
    protected function getExchange(string $exchange = null)
    {
        return $exchange ?: Arr::get($this->options, 'exchange') ?: null;
    }

    /**
     * Get the exchangeType, or AMQPExchangeType::DIRECT as default.
     *
     * @param string|null $type
     *
     * @return string
     */
    protected function getExchangeType(?string $type = null)
    {
        return @constant(AMQPExchangeType::class . '::' . Str::upper($type ?: Arr::get(
                $this->options,
                'exchange_type'
            ) ?: 'direct')) ?: AMQPExchangeType::DIRECT;
    }

    /**
     * Get the Queue arguments.
     * @param string $destination
     * @return array
     */
    protected function getQueueArguments(string $destination)
    {
        $arguments = [];

        // Messages without a priority property are treated as if their priority were 0.
        // Messages with a priority which is higher than the queue's maximum, are treated as if they were
        // published with the maximum priority.
        // Quorum queues does not support priority.
        if ($this->isPrioritizeDelayed() && !$this->isQuorum()) {
            $arguments['x-max-priority'] = $this->getQueueMaxPriority();
        }
        if ($this->isRerouteFailed()) {
            $arguments['x-dead-letter-exchange']    = $this->getFailedExchange() ?? '';
            $arguments['x-dead-letter-routing-key'] = $this->getFailedRoutingKey($destination);
        }

        if ($this->isQuorum()) {
            $arguments['x-queue-type'] = 'quorum';
        }

        return $arguments;
    }

    /**
     * Get the Delay queue arguments.
     * @param string $destination
     * @param int $ttl
     * @return array
     */
    protected function getDelayQueueArguments(string $destination, int $ttl)
    {
        return [
            'x-dead-letter-exchange'    => $this->getExchange() ?? '',
            'x-dead-letter-routing-key' => $this->getRoutingKey($destination),
            'x-message-ttl'             => $ttl,
            'x-expires'                 => $ttl * 2,
        ];
    }

    /**
     * Get the routing-key for failed messages
     * The default routing-key is the given destination substituted by '.failed'.
     * @param string $destination
     * @return string
     */
    protected function getFailedRoutingKey(string $destination)
    {
        return ltrim(sprintf(Arr::get($this->options, 'failed_routing_key') ?: '%s.failed', $destination), '.');
    }

    /**
     * Get the exchange for failed messages.
     * @param string|null $exchange
     * @return string|null
     */
    protected function getFailedExchange(string $exchange = null)
    {
        return $exchange ?: Arr::get($this->options, 'failed_exchange') ?: null;
    }

    /**
     * Returns &true;, if failed messages should be rerouted.
     * @return bool
     */
    protected function isRerouteFailed()
    {
        return Arr::get($this->options, 'reroute_failed', false);
    }

    /**
     * Returns a integer with a default of '2' for when using prioritization on delayed messages.
     * If priority queues are desired, we recommend using between 1 and 10.
     * Using more priority layers, will consume more CPU resources and would affect runtimes.
     * @see https://www.rabbitmq.com/priority.html
     * @return int
     */
    protected function getQueueMaxPriority()
    {
        return Arr::get($this->options, 'queue_max_priority', 2);
    }

    /**
     * Returns &true;, if delayed messages should be prioritized.
     * @return bool
     */
    protected function isPrioritizeDelayed()
    {
        return Arr::get($this->options, 'prioritize_delayed', false);
    }

    /**
     * Returns &true;, if declared queue must be quorum queue.
     * @return bool
     */
    protected function isQuorum()
    {
        return Arr::get($this->options, 'quorum', false);
    }

    /**
     * 获取队列名.
     * @param string|null $queue
     * @return string
     */
    protected function getQueue($queue)
    {
        /** aliyun queue **/
        return $queue ?: $this->default;
    }

    /**
     * 随机id.
     * @return string
     */
    protected function getRandomId()
    {
        return Str::random(32);
    }
}
