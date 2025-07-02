#!/usr/bin/env python
"""
Kafka Dual Topic Performance Test - Simulating Debezium CDC Messages
Tests performance with 2 topics running in parallel: cdc.test.accounts_1 and cdc.test.accounts_2
Each topic can handle customizable message count (default: 4M messages each)
"""

import json
import time
import random
import threading
import argparse
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from confluent_kafka import Producer
from typing import Dict, List, Tuple
import uuid
import clickhouse_connect
import psutil

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Changed from DEBUG to INFO for cleaner output
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DualTopicKafkaPerformanceTest:
    def __init__(self, bootstrap_servers: str, 
                 topic1: str = "cdc.test.accounts_1", 
                 topic2: str = "cdc.test.accounts_2",
                 messages_per_topic: int = 4000000,
                 threads_per_topic: int = 4, 
                 batch_size: int = 1000,
                 partitions_per_topic: int = 3,
                 clickhouse_host: str = "localhost", 
                 clickhouse_port: int = 8123,
                 clickhouse_user: str = "default", 
                 clickhouse_password: str = "giaphu",
                 monitor_clickhouse: bool = True, 
                 clear_tables: bool = False):

        self.bootstrap_servers = bootstrap_servers
        self.topic1 = topic1
        self.topic2 = topic2
        self.messages_per_topic = messages_per_topic
        self.total_messages = messages_per_topic * 2
        self.threads_per_topic = threads_per_topic
        self.total_threads = threads_per_topic * 2
        self.batch_size = batch_size
        self.partitions_per_topic = partitions_per_topic
        self.monitor_clickhouse = monitor_clickhouse
        self.clear_tables = clear_tables

        # ClickHouse connection settings
        self.clickhouse_host = clickhouse_host
        self.clickhouse_port = clickhouse_port
        self.clickhouse_user = clickhouse_user
        self.clickhouse_password = clickhouse_password
        self.clickhouse_client = None

        # Performance tracking - separate for each topic
        self.topic1_stats = {'sent': 0, 'failed': 0, 'bytes': 0}
        self.topic2_stats = {'sent': 0, 'failed': 0, 'bytes': 0}
        self.start_time = 0
        self.end_time = 0
        self.stop_monitoring = False

        # Initial counts for both topics
        self.initial_topic1_count = 0
        self.initial_topic2_count = 0
        self.initial_accounts_count = 0

        # Thread-safe counters
        self.lock = threading.Lock()

        # Cache for count queries to reduce ClickHouse load
        self.count_cache = {
            'topic1': {'value': 0, 'timestamp': 0},
            'topic2': {'value': 0, 'timestamp': 0},
            'accounts': {'value': 0, 'timestamp': 0}
        }
        self.cache_ttl = 3.0  # Cache TTL in seconds

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

    def create_producer(self, client_suffix: str) -> Producer:
        config = {
            'bootstrap.servers': self.bootstrap_servers,
            'client.id': f'debezium-dual-test-{client_suffix}-{str(uuid.uuid4())[:8]}',
            
            'acks': 'all',
            'retries': 2147483647,
            'batch.size': 16384,
            'linger.ms': 0,
            'compression.type': 'snappy',
            'max.in.flight.requests.per.connection': 1,
            'enable.idempotence': True,
            'request.timeout.ms': 30000,
            'delivery.timeout.ms': 2147483647,
        }

        try:
            producer = Producer(config)
            logger.info(f"✅ Kafka producer created for {client_suffix}: {self.bootstrap_servers}")
            return producer
        except Exception as e:
            logger.error(f"❌ Failed to create Kafka producer for {client_suffix}: {e}")
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

            # Check if tables exist
            self.check_tables_exist()

        except Exception as e:
            logger.warning(f"⚠️ ClickHouse connection failed: {e}")
            self.monitor_clickhouse = False

    def check_tables_exist(self):
        """Check if required ClickHouse tables exist"""
        if not self.clickhouse_client:
            return

        try:
            # Check what tables exist
            result = self.clickhouse_client.query("SHOW TABLES")
            existing_tables = [row[0] for row in result.result_rows] if result.result_rows else []
            logger.info(f"📋 Existing ClickHouse tables: {existing_tables}")

            # Check for our specific topics
            for topic in [self.topic1, self.topic2]:
                if topic in existing_tables:
                    logger.info(f"✅ Table {topic} exists")
                else:
                    logger.warning(f"⚠️ Table {topic} does not exist - CDC data won't be available")

            if 'accounts' in existing_tables:
                logger.info("✅ Table accounts exists")
            else:
                logger.warning("⚠️ Table accounts does not exist")

        except Exception as e:
            logger.warning(f"⚠️ Failed to check table existence: {e}")

    def clear_clickhouse_tables(self):
        """Clear ClickHouse tables for both topics before test"""
        if not self.clickhouse_client:
            return

        try:
            logger.info("🧹 Clearing ClickHouse tables for both topics...")

            # Clear both CDC tables
            for topic in [self.topic1, self.topic2]:
                cdc_table = f"`{topic}`"
                self.clickhouse_client.command(f"TRUNCATE TABLE {cdc_table}")
                logger.info(f"  ✅ Cleared CDC table: {cdc_table}")

            # Clear accounts table
            self.clickhouse_client.command("TRUNCATE TABLE accounts")
            logger.info("  ✅ Cleared accounts table")

            time.sleep(2)

            # Verify tables are empty
            topic1_count = self.get_clickhouse_table_count(self.topic1)
            topic2_count = self.get_clickhouse_table_count(self.topic2)
            accounts_count = self.get_accounts_table_count()

            logger.info(f"  📊 After clearing - Topic1: {topic1_count}, Topic2: {topic2_count}, Accounts: {accounts_count}")

        except Exception as e:
            logger.warning(f"⚠️ Failed to clear tables: {e}")

    def capture_initial_counts(self):
        """Capture initial counts for both topics"""
        if not self.clickhouse_client:
            return

        try:
            self.initial_topic1_count = self.get_clickhouse_table_count(self.topic1)
            self.initial_topic2_count = self.get_clickhouse_table_count(self.topic2)
            self.initial_accounts_count = 0

            logger.info(f"📊 Initial counts - Topic1: {self.initial_topic1_count:,}, Topic2: {self.initial_topic2_count:,}")

        except Exception as e:
            logger.warning(f"⚠️ Failed to capture initial counts: {e}")
            self.initial_topic1_count = 0
            self.initial_topic2_count = 0
            self.initial_accounts_count = 0

    def generate_debezium_cdc_message(self, message_id: int, topic_suffix: str) -> tuple[Dict, Dict]:
        """Generate realistic Debezium CDC message for specific topic"""
        # Generate ObjectId-like string with topic suffix for uniqueness
        object_id = f"{topic_suffix}{message_id:020x}"
        
        # Random account data
        account_id = random.randint(100000, 999999)
        limit = random.choice([5000, 10000, 15000, 20000, 25000, 30000])
        products = random.choice(self.products_pool)

        # Operation type distribution
        op_rand = random.random()
        if op_rand < 0.3:
            op = "c"
            deleted = False
        elif op_rand < 0.9:
            op = "u"
            deleted = False
        else:
            op = "d"
            deleted = True

        ts_ms = int(time.time() * 1000)

        key = {"id": object_id}
        
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

    def delivery_callback(self, err, _msg, topic_stats, message_size):
        """Callback for message delivery"""
        if err is not None:
            logger.error(f"Failed to deliver message: {err}")
            topic_stats['failed'] += 1
        else:
            topic_stats['sent'] += 1
            topic_stats['bytes'] += message_size

    def send_batch(self, producer: Producer, topic: str, topic_stats: Dict, 
                   thread_id: int, start_id: int, count: int, topic_suffix: str) -> Dict:
        """Send a batch of messages to specific topic"""
        batch_stats = {'sent': 0, 'failed': 0, 'bytes': 0}

        for i in range(count):
            message_id = start_id + i
            key, value = self.generate_debezium_cdc_message(message_id, topic_suffix)

            key_bytes = json.dumps(key).encode('utf-8')
            value_bytes = json.dumps(value).encode('utf-8')
            message_size = len(key_bytes) + len(value_bytes)

            # Calculate partition or use None to let Kafka decide
            partition = None
            if self.partitions_per_topic > 0:
                partition = message_id % self.partitions_per_topic

            try:
                producer.produce(
                    topic=topic,
                    key=key_bytes,
                    value=value_bytes,
                    partition=partition,
                    callback=lambda err, msg, stats=batch_stats, size=message_size:
                        self.delivery_callback(err, msg, stats, size)
                )

                if i % 100 == 0:
                    producer.poll(0)

            except Exception as e:
                logger.error(f"Failed to produce message {message_id} to {topic}: {e}")
                batch_stats['failed'] += 1

        producer.flush()
        
        # Update topic stats
        with self.lock:
            topic_stats['sent'] += batch_stats['sent']
            topic_stats['failed'] += batch_stats['failed']
            topic_stats['bytes'] += batch_stats['bytes']

        return batch_stats

    def worker_thread(self, thread_id: int, producer: Producer, topic: str, 
                     topic_stats: Dict, start_id: int, messages_count: int, topic_suffix: str):
        """Worker thread to send messages to specific topic"""
        logger.info(f"Thread {thread_id} ({topic}) starting - {messages_count} messages from ID {start_id}")

        remaining = messages_count
        current_start = start_id

        while remaining > 0:
            batch_count = min(self.batch_size, remaining)
            self.send_batch(producer, topic, topic_stats, thread_id, current_start, batch_count, topic_suffix)
            
            remaining -= batch_count
            current_start += batch_count

        logger.info(f"Thread {thread_id} ({topic}) completed")

    def get_all_table_counts(self) -> Tuple[int, int, int]:
        """Get counts for all tables in a single query to reduce ClickHouse load"""
        if not self.clickhouse_client:
            return (0, 0, 0)
            
        current_time = time.time()
        
        # Check if we can use cached values
        if (current_time - self.count_cache['topic1']['timestamp'] < self.cache_ttl and
            current_time - self.count_cache['topic2']['timestamp'] < self.cache_ttl and
            current_time - self.count_cache['accounts']['timestamp'] < self.cache_ttl):
            return (
                self.count_cache['topic1']['value'],
                self.count_cache['topic2']['value'],
                self.count_cache['accounts']['value']
            )
        
        try:
            # Try to get all counts in a single query
            query = f"""
            SELECT 
                (SELECT count() FROM `{self.topic1}`),
                (SELECT count() FROM `{self.topic2}`),
                (SELECT count() FROM accounts)
            """
            
            result = self.clickhouse_client.query(query)
            if result.result_rows:
                topic1_count, topic2_count, accounts_count = result.result_rows[0]
                
                # Update cache
                self.count_cache['topic1'] = {'value': topic1_count, 'timestamp': current_time}
                self.count_cache['topic2'] = {'value': topic2_count, 'timestamp': current_time}
                self.count_cache['accounts'] = {'value': accounts_count, 'timestamp': current_time}
                
                return (topic1_count, topic2_count, accounts_count)
            
            # Fallback to individual queries if combined query fails
            return (
                self.get_clickhouse_table_count(self.topic1),
                self.get_clickhouse_table_count(self.topic2),
                self.get_accounts_table_count()
            )
        except Exception as e:
            logger.debug(f"Failed to query combined table counts: {e}")
            # Fallback to individual queries
            return (
                self.get_clickhouse_table_count(self.topic1),
                self.get_clickhouse_table_count(self.topic2),
                self.get_accounts_table_count()
            )

    def get_clickhouse_table_count(self, topic: str) -> int:
        """Get current record count from specific ClickHouse table with caching"""
        if not self.clickhouse_client:
            return 0
            
        # Check cache first
        cache_key = 'topic1' if topic == self.topic1 else 'topic2'
        current_time = time.time()
        if current_time - self.count_cache[cache_key]['timestamp'] < self.cache_ttl:
            return self.count_cache[cache_key]['value']

        try:
            # Try with backticks first
            table_name = f"`{topic}`"
            try:
                result = self.clickhouse_client.query(f"SELECT count() FROM {table_name}")
                count = result.result_rows[0][0] if result.result_rows else 0
                
                # Update cache
                self.count_cache[cache_key] = {'value': count, 'timestamp': current_time}
                return count
            except Exception as inner_e:
                logger.debug(f"Failed to query {table_name}: {inner_e}")
                
                # Try without backticks
                result = self.clickhouse_client.query(f"SELECT count() FROM {topic}")
                count = result.result_rows[0][0] if result.result_rows else 0
                
                # Update cache
                self.count_cache[cache_key] = {'value': count, 'timestamp': current_time}
                return count

        except Exception as e:
            logger.warning(f"Failed to query {topic} table count: {e}")
            return self.count_cache[cache_key]['value']  # Return last known value

    def get_accounts_table_count(self) -> int:
        """Get current record count from accounts table with caching"""
        if not self.clickhouse_client:
            return 0
            
        # Check cache first
        current_time = time.time()
        if current_time - self.count_cache['accounts']['timestamp'] < self.cache_ttl:
            return self.count_cache['accounts']['value']

        try:
            # Try a more efficient query first (without FINAL)
            result = self.clickhouse_client.query("SELECT count() FROM accounts")
            count = result.result_rows[0][0] if result.result_rows else 0
            
            # Update cache
            self.count_cache['accounts'] = {'value': count, 'timestamp': current_time}
            
            # Check if count is suspiciously exactly 4M
            if count == 4000000 and self.messages_per_topic != 4000000:
                logger.warning(f"⚠️ Account count is exactly 4,000,000 - may be incorrect")
                
            return count
        except Exception as e:
            logger.warning(f"Failed to query accounts table count: {e}")
            return self.count_cache['accounts']['value']  # Return last known value

    def monitor_clickhouse_performance(self):
        """Monitor ClickHouse performance for both topics with optimized queries"""
        logger.info("Starting dual-topic ClickHouse performance monitoring...")
        logger.info(f"Monitoring topics: {self.topic1} and {self.topic2}")

        last_topic1_count = 0
        last_topic2_count = 0
        last_time = time.time()
        
        # Adaptive query interval based on data size
        query_interval = 3.0  # Start with 3 seconds
        
        # Separate interval for accounts table (more expensive query)
        accounts_check_interval = 10.0
        last_accounts_check_time = 0
        accounts_count = 0

        while not self.stop_monitoring:
            try:
                current_time = time.time()
                time_diff = current_time - last_time

                if time_diff >= query_interval:
                    # Get CDC table counts
                    topic1_count, topic2_count, _ = self.get_all_table_counts()
                    
                    # Only check accounts table less frequently
                    if current_time - last_accounts_check_time >= accounts_check_interval:
                        accounts_count = self.get_accounts_table_count()
                        last_accounts_check_time = current_time
                    
                    # Calculate net counts
                    topic1_net_count = max(0, topic1_count - self.initial_topic1_count)
                    topic2_net_count = max(0, topic2_count - self.initial_topic2_count)
                    total_cdc_count = topic1_net_count + topic2_net_count
                    
                    # Calculate rates
                    topic1_diff = topic1_net_count - last_topic1_count
                    topic2_diff = topic2_net_count - last_topic2_count
                    topic1_rate = topic1_diff / time_diff if time_diff > 0 else 0
                    topic2_rate = topic2_diff / time_diff if time_diff > 0 else 0
                    
                    # Get current sent counts
                    with self.lock:
                        topic1_sent = self.topic1_stats['sent']
                        topic2_sent = self.topic2_stats['sent']
                        total_sent = topic1_sent + topic2_sent
                    
                    # Adjust query intervals based on data size
                    if total_cdc_count > 1000000:
                        query_interval = 5.0  # 5 seconds when > 1M records
                        accounts_check_interval = 15.0
                    if total_cdc_count > 5000000:
                        query_interval = 10.0  # 10 seconds when > 5M records
                        accounts_check_interval = 30.0
                    
                    logger.info(
                        f"📊 Sent: {total_sent:,} ({topic1_sent:,}+{topic2_sent:,}) | "
                        f"CDC: {total_cdc_count:,} ({topic1_net_count:,}+{topic2_net_count:,}) | "
                        f"Rates: T1:{topic1_rate:.1f}/s T2:{topic2_rate:.1f}/s | "
                        f"Accounts: {accounts_count:,}"
                    )
                    
                    last_topic1_count = topic1_net_count
                    last_topic2_count = topic2_net_count
                    last_time = current_time
                
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"ClickHouse monitoring error: {e}")
                time.sleep(5)

    def run_test(self):
        """Run the dual-topic performance test"""
        logger.info("🚀 Starting Dual-Topic Kafka Performance Test")
        logger.info(f"Topics: {self.topic1} & {self.topic2}")
        logger.info(f"Messages per topic: {self.messages_per_topic:,}")
        logger.info(f"Total messages: {self.total_messages:,}")
        logger.info(f"Threads per topic: {self.threads_per_topic} (total: {self.total_threads})")
        logger.info(f"Partitions per topic: {self.partitions_per_topic}")
        logger.info(f"Batch size: {self.batch_size}")

        # Setup ClickHouse connection
        self.setup_clickhouse_connection()

        # Clear tables if requested
        if self.clear_tables and self.monitor_clickhouse:
            self.clear_clickhouse_tables()

        # Capture initial counts
        if self.monitor_clickhouse:
            self.capture_initial_counts()

        self.start_time = time.time()

        # Start ClickHouse monitoring
        if self.monitor_clickhouse:
            monitor_thread = threading.Thread(target=self.monitor_clickhouse_performance, daemon=True)
            monitor_thread.start()

        # Create producers for both topics
        topic1_producers = [self.create_producer(f"topic1-{i}") for i in range(self.threads_per_topic)]
        topic2_producers = [self.create_producer(f"topic2-{i}") for i in range(self.threads_per_topic)]

        # Calculate messages per thread for each topic
        messages_per_thread = self.messages_per_topic // self.threads_per_topic
        remainder = self.messages_per_topic % self.threads_per_topic

        # Start worker threads for both topics
        with ThreadPoolExecutor(max_workers=self.total_threads) as executor:
            futures = []

            # Topic 1 threads
            current_start_id = 0
            for i in range(self.threads_per_topic):
                thread_messages = messages_per_thread + (1 if i < remainder else 0)
                future = executor.submit(
                    self.worker_thread,
                    f"T1-{i}",
                    topic1_producers[i],
                    self.topic1,
                    self.topic1_stats,
                    current_start_id,
                    thread_messages,
                    "1"  # topic suffix
                )
                futures.append(future)
                current_start_id += thread_messages

            # Topic 2 threads
            current_start_id = 0
            for i in range(self.threads_per_topic):
                thread_messages = messages_per_thread + (1 if i < remainder else 0)
                future = executor.submit(
                    self.worker_thread,
                    f"T2-{i}",
                    topic2_producers[i],
                    self.topic2,
                    self.topic2_stats,
                    current_start_id,
                    thread_messages,
                    "2"  # topic suffix
                )
                futures.append(future)
                current_start_id += thread_messages

            # Wait for all threads to complete
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Thread failed with error: {e}")

        self.end_time = time.time()

        # Close all producers
        for producer in topic1_producers + topic2_producers:
            producer.flush()

        # Wait for ClickHouse to process remaining messages
        if self.monitor_clickhouse:
            logger.info("Waiting for ClickHouse to process remaining messages...")
            self.wait_for_cdc_completion()

        self.stop_monitoring = True
        self.print_results()

    def print_results(self):
        """Print comprehensive dual-topic performance test results"""
        duration = self.end_time - self.start_time

        logger.info("=" * 80)
        logger.info("DUAL-TOPIC KAFKA-CLICKHOUSE PERFORMANCE TEST RESULTS")
        logger.info("🔄 SIMULATED DEBEZIUM CDC MESSAGES")
        logger.info("=" * 80)

        # Combined Kafka Producer Results
        total_sent = self.topic1_stats['sent'] + self.topic2_stats['sent']
        total_failed = self.topic1_stats['failed'] + self.topic2_stats['failed']
        total_bytes = self.topic1_stats['bytes'] + self.topic2_stats['bytes']

        logger.info("📤 KAFKA PRODUCER METRICS (COMBINED):")
        logger.info(f"  Total messages sent: {total_sent:,}")
        logger.info(f"  Total messages failed: {total_failed:,}")
        logger.info(f"  Total bytes sent: {total_bytes:,} ({total_bytes/1024/1024:.2f} MB)")
        logger.info(f"  Test duration: {duration:.2f} seconds")
        logger.info(f"  Producer rate: {total_sent/duration:.2f} msg/s")
        logger.info(f"  Producer throughput: {(total_bytes/1024/1024)/duration:.2f} MB/s")

        # Individual topic results
        logger.info("")
        logger.info("📤 INDIVIDUAL TOPIC METRICS:")
        logger.info(f"  Topic 1 ({self.topic1}):")
        logger.info(f"    Messages sent: {self.topic1_stats['sent']:,}")
        logger.info(f"    Messages failed: {self.topic1_stats['failed']:,}")
        logger.info(f"    Bytes sent: {self.topic1_stats['bytes']:,} ({self.topic1_stats['bytes']/1024/1024:.2f} MB)")

        logger.info(f"  Topic 2 ({self.topic2}):")
        logger.info(f"    Messages sent: {self.topic2_stats['sent']:,}")
        logger.info(f"    Messages failed: {self.topic2_stats['failed']:,}")
        logger.info(f"    Bytes sent: {self.topic2_stats['bytes']:,} ({self.topic2_stats['bytes']/1024/1024:.2f} MB)")

        # ClickHouse Results
        if self.monitor_clickhouse:
            logger.info("")
            logger.info("🗄️ CLICKHOUSE SINK METRICS:")

            # Get final counts with direct queries to ensure accuracy
            try:
                # Force refresh cache with direct queries
                self.cache_ttl = -1  # Disable cache temporarily
                
                # Get counts directly
                topic1_total = self.get_clickhouse_table_count(self.topic1)
                topic2_total = self.get_clickhouse_table_count(self.topic2)
                
                # For accounts, try both with and without FINAL
                try:
                    accounts_total = self.clickhouse_client.query("SELECT count() FROM accounts").result_rows[0][0]
                    accounts_total_final = self.clickhouse_client.query("SELECT count() FROM accounts FINAL").result_rows[0][0]
                    logger.info(f"  Accounts count: {accounts_total:,} (without FINAL: {accounts_total:,}, with FINAL: {accounts_total_final:,})")
                except Exception:
                    accounts_total = self.get_accounts_table_count()
                
                # Calculate net changes
                topic1_net = max(0, topic1_total - self.initial_topic1_count)
                topic2_net = max(0, topic2_total - self.initial_topic2_count)
                total_cdc_net = topic1_net + topic2_net
                
                logger.info(f"  Messages sent: {total_sent:,}")
                logger.info(f"  CDC processed: {total_cdc_net:,} ({topic1_net:,}+{topic2_net:,})")
                logger.info(f"  Accounts current: {accounts_total:,}")
                
                cdc_remaining = total_sent - total_cdc_net
                logger.info(f"  CDC remaining: {cdc_remaining:,} messages")
                
                # Restore cache TTL
                self.cache_ttl = 3.0
                
            except Exception as e:
                logger.warning(f"Failed to get final counts: {e}")
                # Use cached values as fallback
                topic1_total = self.count_cache['topic1']['value']
                topic2_total = self.count_cache['topic2']['value']
                accounts_total = self.count_cache['accounts']['value']
                
                topic1_net = max(0, topic1_total - self.initial_topic1_count)
                topic2_net = max(0, topic2_total - self.initial_topic2_count)
                total_cdc_net = topic1_net + topic2_net
                
                logger.info(f"  Messages sent: {total_sent:,}")
                logger.info(f"  CDC processed (from cache): {total_cdc_net:,} ({topic1_net:,}+{topic2_net:,})")
                logger.info(f"  Accounts current (from cache): {accounts_total:,}")

        # System metrics
        logger.info("")
        logger.info("💻 SYSTEM METRICS:")
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        logger.info(f"  CPU usage: {cpu_percent:.1f}%")
        logger.info(f"  Memory usage: {memory.percent:.1f}% ({memory.used/1024/1024/1024:.1f}GB used)")

        logger.info("=" * 80)

    def wait_for_cdc_completion(self):
        """Wait until all CDC messages have been processed"""
        logger.info("Waiting for all CDC messages to be processed...")
        
        # Set a reasonable timeout (e.g., 10 minutes)
        max_wait_time = 600  # seconds
        start_wait_time = time.time()
        check_interval = 5  # seconds
        
        last_topic1_count = 0
        last_topic2_count = 0
        no_change_count = 0
        
        while True:
            # Get current counts
            topic1_count, topic2_count, accounts_count = self.get_all_table_counts()
            
            # Calculate net counts
            topic1_net = max(0, topic1_count - self.initial_topic1_count)
            topic2_net = max(0, topic2_count - self.initial_topic2_count)
            total_cdc_net = topic1_net + topic2_net
            
            # Calculate remaining messages
            total_sent = self.topic1_stats['sent'] + self.topic2_stats['sent']
            cdc_remaining = total_sent - total_cdc_net
            
            # Log current status
            logger.info(f"CDC processed: {total_cdc_net:,} ({topic1_net:,}+{topic2_net:,})")
            logger.info(f"Accounts current: {accounts_count:,}")
            logger.info(f"CDC remaining: {cdc_remaining:,} messages")
            
            # Check if all messages processed
            if cdc_remaining <= 0:
                logger.info("✅ All CDC messages have been processed!")
                break
                
            # Check for no progress (stuck)
            if topic1_net == last_topic1_count and topic2_net == last_topic2_count:
                no_change_count += 1
                if no_change_count >= 6:  # No change for 30 seconds
                    logger.warning("⚠️ No progress detected for 30 seconds, may be stuck")
                    logger.info("Continuing to wait...")
                    no_change_count = 0  # Reset counter but continue waiting
            else:
                no_change_count = 0
                
            # Check timeout
            if time.time() - start_wait_time > max_wait_time:
                logger.warning(f"⚠️ Timeout after {max_wait_time} seconds")
                logger.warning(f"CDC remaining: {cdc_remaining:,} messages")
                break
                
            # Update last counts
            last_topic1_count = topic1_net
            last_topic2_count = topic2_net
            
            # Wait before next check
            time.sleep(check_interval)

def main():
    parser = argparse.ArgumentParser(description='Dual-Topic Kafka Performance Test with ClickHouse Monitoring')

    parser.add_argument('--servers', default='localhost:29092',
                       help='Kafka bootstrap servers (default: localhost:29092)')
    parser.add_argument('--topic1', default='cdc.test.accounts_1',
                       help='First Kafka topic (default: cdc.test.accounts_1)')
    parser.add_argument('--topic2', default='cdc.test.accounts_2',
                       help='Second Kafka topic (default: cdc.test.accounts_2)')
    parser.add_argument('--messages-per-topic', type=int, default=4000000,
                       help='Messages per topic (default: 4,000,000)')
    parser.add_argument('--threads-per-topic', type=int, default=4,
                       help='Producer threads per topic (default: 4)')
    parser.add_argument('--partitions-per-topic', type=int, default=3,
                       help='Partitions per topic (default: 3)')
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
        test = DualTopicKafkaPerformanceTest(
            bootstrap_servers=args.servers,
            topic1=args.topic1,
            topic2=args.topic2,
            messages_per_topic=args.messages_per_topic,
            threads_per_topic=args.threads_per_topic,
            batch_size=args.batch_size,
            partitions_per_topic=args.partitions_per_topic,
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



