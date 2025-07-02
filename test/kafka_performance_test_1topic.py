#!/usr/bin/env python3
"""
Kafka Performance Test - Simulating Debezium CDC Messages
Measures performance of Kafka producer and ClickHouse sink connector
"""

import json
import time
import random
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from confluent_kafka import Producer
import argparse
import logging
from typing import Dict
import uuid
import clickhouse_connect
import psutil

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class KafkaPerformanceTest:
    def __init__(self, bootstrap_servers: str, topic: str, total_messages: int,
                 num_threads: int = 10, batch_size: int = 1000,
                 clickhouse_host: str = "localhost", clickhouse_port: int = 8123,
                 clickhouse_user: str = "default", clickhouse_password: str = "giaphu",
                 monitor_clickhouse: bool = True, clear_tables: bool = False):

        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.total_messages = total_messages
        self.num_threads = num_threads
        self.batch_size = batch_size
        self.monitor_clickhouse = monitor_clickhouse
        self.clear_tables = clear_tables

        # ClickHouse connection settings
        self.clickhouse_host = clickhouse_host
        self.clickhouse_port = clickhouse_port
        self.clickhouse_user = clickhouse_user
        self.clickhouse_password = clickhouse_password
        self.clickhouse_client = None

        # Performance tracking
        self.messages_sent = 0
        self.messages_failed = 0
        self.total_bytes_sent = 0
        self.start_time = 0
        self.end_time = 0
        self.stop_monitoring = False

        # Initial counts to exclude old data
        self.initial_cdc_count = 0
        self.initial_accounts_count = 0

        # Lag tracking
        self.lag_measurements = []
        self.lag_lock = threading.Lock()
        self.current_lag = 0
        self.average_lag = 0

        # Thread-safe counters
        self.lock = threading.Lock()

        # Sample products for variety
        self.products_pool = [
            ["InvestmentFund", "InvestmentStock"],
            ["Commodity", "Derivatives"],
            ["CreditCard", "Loan"],
            ["Savings", "Checking"],
            ["InvestmentFund"],
            ["Commodity"],
            ["CreditCard"],
            ["Savings"]
        ]

    def create_producer(self) -> Producer:
        config = {
            'bootstrap.servers': self.bootstrap_servers,
            'client.id': f'debezium-perf-test-{str(uuid.uuid4())[:8]}',

            'acks': 'all',            
            'retries': 2147483647,       
            'batch.size': 16384,       
            'linger.ms': 30,             
            'compression.type': 'snappy', # Fast compression
            'max.in.flight.requests.per.connection': 1, 
            'enable.idempotence': True,   # Exactly-once semantics
            'request.timeout.ms': 30000,
            'delivery.timeout.ms': 2147483647,  # Max timeout
        }

        try:
            producer = Producer(config)
            logger.info(f"✅ Kafka producer created with Debezium config: {self.bootstrap_servers}")
            return producer
        except Exception as e:
            logger.error(f"❌ Failed to create Kafka producer: {e}")
            raise

    def setup_clickhouse_connection(self):
        """Setup ClickHouse connection for monitoring"""
        if not self.monitor_clickhouse:
            return

        try:
            self.clickhouse_client = clickhouse_connect.get_client(
                host=self.clickhouse_host,
                port=self.clickhouse_port,
                username=self.clickhouse_user,
                password=self.clickhouse_password
            )
            logger.info(f"✅ ClickHouse connection established: {self.clickhouse_host}:{self.clickhouse_port}")
        except Exception as e:
            logger.warning(f"⚠️ ClickHouse connection failed: {e}")
            self.monitor_clickhouse = False

    def clear_clickhouse_tables(self):
        """Clear ClickHouse tables before test"""
        if not self.clickhouse_client:
            return

        try:
            logger.info("🧹 Clearing ClickHouse tables...")

            # Truncate CDC table
            cdc_table = f"`{self.topic}`"
            self.clickhouse_client.command(f"TRUNCATE TABLE {cdc_table}")
            logger.info(f"  ✅ Cleared CDC table: {cdc_table}")

            # Truncate accounts table
            self.clickhouse_client.command("TRUNCATE TABLE accounts")
            logger.info("  ✅ Cleared accounts table")

            # Wait a moment for truncation to complete
            time.sleep(2)

            # Verify tables are empty
            cdc_count = self.get_clickhouse_table_count()
            accounts_count = self.get_accounts_table_count()

            logger.info(f"  📊 After clearing - CDC: {cdc_count}, Accounts: {accounts_count}")

        except Exception as e:
            logger.warning(f"⚠️ Failed to clear tables: {e}")

    def capture_initial_counts(self):
        """Capture initial counts to exclude old data from efficiency calculations"""
        if not self.clickhouse_client:
            return

        try:
            self.initial_cdc_count = self.get_clickhouse_table_count()
            # Don't subtract initial accounts count since ReplacingMergeTree with FINAL
            # already shows the correct current state
            self.initial_accounts_count = 0

            logger.info(f"📊 Initial CDC count: {self.initial_cdc_count:,} (will track net CDC changes)")

        except Exception as e:
            logger.warning(f"⚠️ Failed to capture initial counts: {e}")
            self.initial_cdc_count = 0
            self.initial_accounts_count = 0

    def generate_debezium_cdc_message(self, message_id: int) -> tuple[Dict, Dict]:
        """Generate realistic Debezium CDC message matching the provided format"""
        # Generate ObjectId-like string (24 hex characters)
        object_id = f"{message_id:024x}"

        # Random account data
        account_id = random.randint(100000, 999999)
        limit = random.choice([5000, 10000, 15000, 20000, 25000, 30000])
        products = random.choice(self.products_pool)

        # Operation type distribution: 30% create, 60% update, 10% delete
        op_rand = random.random()
        if op_rand < 0.3:
            op = "c"  # create
            deleted = False
        elif op_rand < 0.9:
            op = "u"  # update
            deleted = False
        else:
            op = "d"  # delete
            deleted = True

        # Current timestamp in milliseconds
        ts_ms = int(time.time() * 1000)

        # Kafka key (separate from value)
        key = {
            "id": object_id
        }

        # Kafka value (main message content)
        value = {
            "_id": object_id,
            "account_id": account_id,
            "limit": limit,
            "products": products,
            "__deleted": deleted,
            "__op": op,
            "__ts_ms": ts_ms,
            "kafkaKey": object_id
        }

        return key, value

    def delivery_callback(self, err, _msg, batch_stats, message_size):
        """Callback for message delivery"""
        if err is not None:
            logger.error(f"Failed to deliver message: {err}")
            batch_stats['failed'] += 1
        else:
            batch_stats['sent'] += 1
            batch_stats['bytes'] += message_size

            if batch_stats['sent'] % 10000 == 0:
                logger.info(f"Sent {batch_stats['sent']} messages in current batch")

    def send_batch(self, producer: Producer, thread_id: int, start_id: int, count: int) -> Dict:
        """Send a batch of messages"""
        batch_stats = {'sent': 0, 'failed': 0, 'bytes': 0}

        for i in range(count):
            message_id = start_id + i
            key, value = self.generate_debezium_cdc_message(message_id)

            # Serialize to JSON
            key_bytes = json.dumps(key).encode('utf-8')
            value_bytes = json.dumps(value).encode('utf-8')
            message_size = len(key_bytes) + len(value_bytes)

            # Calculate partition (distribute across 8 partitions)
            partition = message_id % 6

            try:
                producer.produce(
                    topic=self.topic,
                    key=key_bytes,
                    value=value_bytes,
                    partition=partition,
                    callback=lambda err, msg, stats=batch_stats, size=message_size:
                        self.delivery_callback(err, msg, stats, size)
                )

                # Poll for delivery reports periodically
                if i % 100 == 0:
                    producer.poll(0)

            except Exception as e:
                logger.error(f"Failed to produce message {message_id}: {e}")
                batch_stats['failed'] += 1

        # Final poll to ensure all messages are sent
        producer.flush()

        return batch_stats

    def worker_thread(self, thread_id: int, producer: Producer, start_id: int, messages_count: int):
        """Worker thread to send messages"""
        logger.info(f"Thread {thread_id} starting - will send {messages_count} messages starting from ID {start_id}")

        # Send messages in batches
        total_stats = {'sent': 0, 'failed': 0, 'bytes': 0}

        remaining = messages_count
        current_start = start_id

        while remaining > 0:
            batch_count = min(self.batch_size, remaining)
            batch_stats = self.send_batch(producer, thread_id, current_start, batch_count)

            # Update totals
            total_stats['sent'] += batch_stats['sent']
            total_stats['failed'] += batch_stats['failed']
            total_stats['bytes'] += batch_stats['bytes']

            # Update global counters
            with self.lock:
                self.messages_sent += batch_stats['sent']
                self.messages_failed += batch_stats['failed']
                self.total_bytes_sent += batch_stats['bytes']

            remaining -= batch_count
            current_start += batch_count

        logger.info(f"Thread {thread_id} completed - sent {total_stats['sent']}, failed {total_stats['failed']}")
        return total_stats

    def get_clickhouse_table_count(self) -> int:
        """Get current record count from ClickHouse table"""
        if not self.clickhouse_client:
            return 0

        try:
            # Use the actual CDC table name that exists
            cdc_table = f"`{self.topic}`"
            result = self.clickhouse_client.query(f"SELECT count() FROM {cdc_table}")
            return result.result_rows[0][0] if result.result_rows else 0
        except Exception as e:
            logger.warning(f"Failed to query ClickHouse table count: {e}")
            return 0

    def get_accounts_table_count(self) -> int:
        """Get current record count from accounts ReplacingMergeTree table"""
        if not self.clickhouse_client:
            return 0

        try:
            # Query the final accounts table (ReplacingMergeTree)
            result = self.clickhouse_client.query("SELECT count() FROM accounts FINAL")
            return result.result_rows[0][0] if result.result_rows else 0
        except Exception as e:
            logger.warning(f"Failed to query accounts table count: {e}")
            return 0

    def calculate_lag(self):
        """Calculate current lag between Kafka and ClickHouse"""
        try:
            if not self.clickhouse_client:
                return 0

            # Get the latest timestamp from ClickHouse CDC table
            cdc_table = f"`{self.topic}`"
            result = self.clickhouse_client.query(
                f"SELECT max(__ts_ms) FROM {cdc_table} WHERE __ts_ms > 0"
            )

            if result.result_rows and result.result_rows[0][0]:
                latest_ch_timestamp = result.result_rows[0][0]
                current_time_ms = int(time.time() * 1000)
                lag_ms = current_time_ms - latest_ch_timestamp

                # Store lag measurement
                with self.lag_lock:
                    self.lag_measurements.append(lag_ms)
                    # Keep only last 100 measurements for average
                    if len(self.lag_measurements) > 100:
                        self.lag_measurements.pop(0)

                    self.current_lag = lag_ms
                    self.average_lag = sum(self.lag_measurements) / len(self.lag_measurements)

                return lag_ms

        except Exception as e:
            logger.debug(f"Lag calculation error: {e}")

        return 0

    def monitor_clickhouse_performance(self):
        """Monitor ClickHouse performance and lag in background"""
        logger.info("Starting ClickHouse performance monitoring with lag tracking...")

        last_count = 0
        last_time = time.time()

        while not self.stop_monitoring:
            try:
                current_time = time.time()
                time_diff = current_time - last_time

                if time_diff >= 3.0:  # Update every 3 seconds
                    # Get counts from both CDC table and final accounts table
                    cdc_total_count = self.get_clickhouse_table_count()
                    accounts_total_count = self.get_accounts_table_count()

                    # Calculate net counts (only subtract initial CDC count)
                    cdc_net_count = max(0, cdc_total_count - self.initial_cdc_count)
                    accounts_net_count = accounts_total_count  # Use actual count for accounts

                    records_diff = cdc_net_count - last_count
                    records_per_sec = records_diff / time_diff if time_diff > 0 else 0

                    # Calculate lag
                    current_lag_ms = self.calculate_lag()

                    # Format lag display
                    current_lag_sec = current_lag_ms / 1000.0
                    avg_lag_sec = self.average_lag / 1000.0

                    logger.info(
                        f"📊 Sent: {self.messages_sent:,} | "
                        f"CDC: {cdc_net_count:,} ({records_per_sec:.1f}/s) | "
                        f"Accounts: {accounts_net_count:,} | "
                        f"🕐 Lag: {current_lag_sec:.2f}s (avg: {avg_lag_sec:.2f}s)"
                    )

                    last_count = cdc_net_count
                    last_time = current_time

                time.sleep(1)

            except Exception as e:
                logger.error(f"ClickHouse monitoring error: {e}")
                time.sleep(5)

    def run_test(self):
        """Run the performance test"""
        logger.info("🚀 Starting Kafka Performance Test with Simulated Debezium CDC Messages")
        logger.info(f"Target messages: {self.total_messages:,}")
        logger.info(f"Kafka topic: {self.topic} (6 partitions)")
        logger.info(f"Threads: {self.num_threads}")
        logger.info(f"Batch size: {self.batch_size}")

        # Setup ClickHouse connection
        self.setup_clickhouse_connection()

        # Clear tables if requested
        if self.clear_tables and self.monitor_clickhouse:
            self.clear_clickhouse_tables()

        # Capture initial counts to exclude old data
        if self.monitor_clickhouse:
            self.capture_initial_counts()

        self.start_time = time.time()

        # Start ClickHouse monitoring
        if self.monitor_clickhouse:
            monitor_thread = threading.Thread(target=self.monitor_clickhouse_performance, daemon=True)
            monitor_thread.start()

        # Create producers for each thread
        producers = [self.create_producer() for _ in range(self.num_threads)]

        # Calculate messages per thread
        messages_per_thread = self.total_messages // self.num_threads
        remainder = self.total_messages % self.num_threads

        # Start worker threads
        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            futures = []
            current_start_id = 0

            for i in range(self.num_threads):
                # Add remainder to first few threads
                thread_messages = messages_per_thread + (1 if i < remainder else 0)

                future = executor.submit(
                    self.worker_thread,
                    i,
                    producers[i],
                    current_start_id,
                    thread_messages
                )
                futures.append(future)
                current_start_id += thread_messages

            # Wait for all threads to complete
            for future in as_completed(futures):
                try:
                    future.result()  # Wait for completion
                except Exception as e:
                    logger.error(f"Thread failed with error: {e}")

        self.end_time = time.time()

        # Close all producers
        for producer in producers:
            producer.flush()

        # Wait a bit for ClickHouse to process remaining messages
        if self.monitor_clickhouse:
            logger.info("Waiting for ClickHouse to process remaining messages...")
            time.sleep(10)

        self.print_results()

    def print_results(self):
        """Print comprehensive performance test results"""
        duration = self.end_time - self.start_time

        logger.info("=" * 80)
        logger.info("KAFKA-CLICKHOUSE PERFORMANCE TEST RESULTS")
        logger.info("🔄 SIMULATED DEBEZIUM CDC MESSAGES")
        logger.info("=" * 80)

        # Kafka Producer Results
        logger.info("📤 KAFKA PRODUCER METRICS:")
        logger.info(f"  Total messages sent: {self.messages_sent:,}")
        logger.info(f"  Total messages failed: {self.messages_failed:,}")
        logger.info(f"  Total bytes sent: {self.total_bytes_sent:,} ({self.total_bytes_sent/1024/1024:.2f} MB)")
        logger.info(f"  Test duration: {duration:.2f} seconds")
        logger.info(f"  Producer rate: {self.messages_sent/duration:.2f} msg/s")
        logger.info(f"  Producer throughput: {(self.total_bytes_sent/1024/1024)/duration:.2f} MB/s")
        logger.info(f"  Success rate: {(self.messages_sent/(self.messages_sent + self.messages_failed))*100:.2f}%")

        # ClickHouse Results
        if self.monitor_clickhouse:
            logger.info("")
            logger.info("🗄️ CLICKHOUSE SINK METRICS:")

            # Get total counts from both tables
            cdc_total_records = self.get_clickhouse_table_count()
            accounts_total_records = self.get_accounts_table_count()

            # Calculate net counts (only subtract initial CDC count)
            cdc_net_records = max(0, cdc_total_records - self.initial_cdc_count)
            accounts_net_records = accounts_total_records  # Use actual count for accounts

            logger.info(f"  Messages sent: {self.messages_sent:,}")
            logger.info(f"  CDC processed: {cdc_net_records:,} (total: {cdc_total_records:,})")
            logger.info(f"  Accounts current: {accounts_net_records:,}")

            # Simple message counts without percentages
            cdc_remaining = self.messages_sent - cdc_net_records

            logger.info(f"  CDC remaining: {cdc_remaining:,} messages")

            # Time-based lag statistics
            logger.info("")
            logger.info("🕐 LAG STATISTICS:")
            if self.lag_measurements:
                current_lag_sec = self.current_lag / 1000.0
                avg_lag_sec = self.average_lag / 1000.0
                min_lag_sec = min(self.lag_measurements) / 1000.0
                max_lag_sec = max(self.lag_measurements) / 1000.0

                logger.info(f"  Current lag: {current_lag_sec:.2f} seconds")
                logger.info(f"  Average lag: {avg_lag_sec:.2f} seconds")
                logger.info(f"  Min lag: {min_lag_sec:.2f} seconds")
                logger.info(f"  Max lag: {max_lag_sec:.2f} seconds")
                logger.info(f"  Lag measurements: {len(self.lag_measurements)} samples")
            else:
                logger.info("  No lag measurements available")

        # System metrics
        logger.info("")
        logger.info("💻 SYSTEM METRICS:")
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        logger.info(f"  CPU usage: {cpu_percent:.1f}%")
        logger.info(f"  Memory usage: {memory.percent:.1f}% ({memory.used/1024/1024/1024:.1f}GB used)")

        logger.info("=" * 80)

def main():
    parser = argparse.ArgumentParser(description='Kafka Performance Test with ClickHouse Sink Connector Monitoring')

    parser.add_argument('--servers', default='localhost:29092',
                       help='Kafka bootstrap servers (default: localhost:29092)')
    parser.add_argument('--topic', default='cdc.test.accounts',
                       help='Kafka topic (default: cdc.test.accounts)')
    parser.add_argument('--messages', type=int, default=8000000,
                       help='Total number of messages to send (default: 8,000,000)')
    parser.add_argument('--threads', type=int, default=8,
                       help='Number of producer threads (default: 10)')
    parser.add_argument('--batch-size', type=int, default=1000,
                       help='Batch size for sending messages (default: 1000)')
    parser.add_argument('--clickhouse-host', default='localhost',
                       help='ClickHouse host (default: localhost)')
    parser.add_argument('--clickhouse-port', type=int, default=8123,
                       help='ClickHouse port (default: 8123)')
    parser.add_argument('--clickhouse-user', default='default',
                       help='ClickHouse username (default: default)')
    parser.add_argument('--clickhouse-password', default='giaphu',
                       help='ClickHouse password (default: giaphu)')
    parser.add_argument('--no-clickhouse-monitoring', action='store_true',
                       help='Disable ClickHouse monitoring')
    parser.add_argument('--clear-tables', action='store_true',
                       help='Clear ClickHouse tables before test')

    args = parser.parse_args()

    try:
        test = KafkaPerformanceTest(
            bootstrap_servers=args.servers,
            topic=args.topic,
            total_messages=args.messages,
            num_threads=args.threads,
            batch_size=args.batch_size,
            clickhouse_host=args.clickhouse_host,
            clickhouse_port=args.clickhouse_port,
            clickhouse_user=args.clickhouse_user,
            clickhouse_password=args.clickhouse_password,
            monitor_clickhouse=not args.no_clickhouse_monitoring,
            clear_tables=args.clear_tables
        )

        test.run_test()

    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
    except Exception as e:
        logger.error(f"Test failed with error: {e}")
        raise

if __name__ == "__main__":
    main()