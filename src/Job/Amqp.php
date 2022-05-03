<?php

namespace think\queue\job;


use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Exception\AMQPProtocolChannelException;
use PhpAmqpLib\Message\AMQPMessage;
use think\App;
use think\helper\Arr;
use think\queue\Job;
use tp5er\think\queue\Connector\AmqpConnector;


class Amqp extends Job
{
    /**
     * @var AMQPMessage
     */
    protected $message;
    /**
     * @var AmqpConnector
     */
    protected $connector;
    /**
     * @var array
     */
    protected $decoded;

    /**
     * Amqp constructor.
     * @param App $app
     * @param AmqpConnector $connector
     * @param AMQPStreamConnection $connection
     * @param AMQPMessage $message
     * @param $queue
     */
    public function __construct(App $app, AmqpConnector $connector, AMQPStreamConnection $connection, AMQPMessage $message, $queue)
    {
        $this->connector  = $connector;
        $this->app        = $app;
        $this->message    = $message;
        $this->queue      = $queue;
        $this->connection = $connection;
        $this->decoded    = $this->payload();
    }

    /**
     * Get the job identifier.
     * @return string
     */
    public function getJobId()
    {
        return $this->decoded['id'] ?? null;
    }

    /**
     * Get the number of times the job has been attempted.
     *
     * @return int
     */
    public function attempts()
    {
        return ($this->decoded['attempts'] ?? null) + 1;
//        if (!$data = $this->getMessageHeaders()) {
//            return 1;
//        }
//        $attempts = (int)Arr::get($data, 'thinkphp.attempts', 0);
//        return $attempts + 1;
    }

    /**
     * 删除任务
     * @return void
     */
    public function delete()
    {
        parent::delete();
        // When delete is called and the Job was not failed, the message must be acknowledged.
        // This is because this is a controlled call by a developer. So the message was handled correct.
        if (!$this->failed) {
            $this->connector->ack($this);
        }
    }

    /**
     * 重新发布任务
     * @param int $delay
     * @return void
     * @throws AMQPProtocolChannelException
     */
    public function release($delay = 0)
    {
        parent::release($delay);
        $this->connector->laterRaw($delay, $this->message->getBody(), $this->queue, $this->attempts());
        $this->connector->ack($this);
    }

    /**
     * Get the raw body string for the job.
     * @return string
     */
    public function getRawBody()
    {
        return $this->message->getBody();
    }


    /**
     * @return |null
     */
    public function getMessageHeaders()
    {
        if (!$headers = Arr::get($this->message->get_properties(), 'application_headers')) {
            return null;
        }
        return $headers->getNativeData();
    }

    /**
     * @return AMQPMessage
     */
    public function getMessage()
    {
        return $this->message;
    }
}
