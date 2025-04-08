import pika
import redis
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Orchestrator")

class Orchestrator:
    def __init__(self):
        self.redis_host = 'redis'
        self.redis_port = 6379
        self.rabbitmq_host = 'rabbitmq'
        self.rabbitmq_user = 'myuser'
        self.rabbitmq_pass = 'mypassword'
        self.redis_client = None
        self.rabbitmq_connection = None
        self.rabbitmq_channel = None
        self.fifo_queue = 'fifo_queue'
        self.processor_queue = 'processor_queue'
        self.processor_available_queue = 'processor_available_queue'
        self.redis_list_key = 'fifo_agents'

    def connect_redis(self):
        self.redis_client = redis.Redis(host=self.redis_host, port=self.redis_port, db=0)
        try:
            self.redis_client.ping()
            logger.info("Connected to Redis")
        except redis.ConnectionError:
            logger.error("Failed to connect to Redis")
            raise
        
    def connect_rabbitmq(self):
        credentials = pika.PlainCredentials(self.rabbitmq_user, self.rabbitmq_pass)
        parameters = pika.ConnectionParameters(
            host=self.rabbitmq_host,
            credentials=credentials,
            heartbeat=600,
            blocked_connection_timeout=300
        )
        self.rabbitmq_connection = pika.BlockingConnection(parameters)
        self.rabbitmq_channel = self.rabbitmq_connection.channel()
        self.rabbitmq_channel.queue_declare(queue=self.fifo_queue, durable=True)
        self.rabbitmq_channel.queue_declare(queue=self.processor_queue, durable=True)
        self.rabbitmq_channel.queue_declare(queue=self.processor_available_queue, durable=True)
        logger.info("Connected to RabbitMQ")

    def on_fifo_message(self, ch, method, properties, body):
        try:
            agent_id = body.decode()
            logger.info(f"Received agent {agent_id} from fifo_queue")
            self.redis_client.rpush(self.redis_list_key, agent_id)
            logger.info(f"Added agent {agent_id} to Redis list")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            self.process_next_agent()
        except Exception as e:
            logger.error(f"Error processing fifo message: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def on_processor_available(self, ch, method, properties, body):
        logger.info("Received processor available signal")
        self.process_next_agent()

    def process_next_agent(self):
        try:
            if self.is_processor_available():
                agent_id = self.redis_client.lpop(self.redis_list_key)
                if agent_id:
                    agent_id = agent_id.decode()
                    logger.info(f"Sending agent {agent_id} to processor_queue")
                    self.rabbitmq_channel.basic_publish(
                        exchange='',
                        routing_key=self.processor_queue,
                        body=agent_id,
                        properties=pika.BasicProperties(delivery_mode=2))
        except Exception as e:
            logger.error(f"Error processing next agent: {e}")

    def is_processor_available(self):
        try:
            queue = self.rabbitmq_channel.queue_declare(queue=self.processor_queue, passive=True)
            return queue.method.message_count == 0
        except pika.exceptions.ChannelClosedByBroker as e:
            if e.reply_code == 404:
                return True
            raise
        except Exception as e:
            logger.error(f"Error checking processor_queue: {e}")
            return False

    def start_consuming(self):
        self.rabbitmq_channel.basic_consume(
            queue=self.fifo_queue,
            on_message_callback=self.on_fifo_message,
            auto_ack=False
        )
        self.rabbitmq_channel.basic_consume(
            queue=self.processor_available_queue,
            on_message_callback=self.on_processor_available,
            auto_ack=True
        )
        logger.info("Orchestrator started. Waiting for messages...")
        self.rabbitmq_channel.start_consuming()

    def run(self):
        self.connect_redis()
        self.connect_rabbitmq()
        self.start_consuming()

if __name__ == "__main__":
    orchestrator = Orchestrator()
    try:
        orchestrator.run()
    except KeyboardInterrupt:
        logger.info("Orchestrator stopped gracefully")
    except Exception as e:
        logger.error(f"Orchestrator failed: {e}")