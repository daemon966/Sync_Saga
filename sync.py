"""
Parallel Data Processing Pipeline
----------------------------------
Demonstrates:
- Multiprocessing for CPU tasks
- ThreadPoolExecutor for IO tasks
- Batching
- Logging
- Benchmarking
- Config management
- Graceful shutdown
"""

import os
import sys
import time
import math
import json
import queue
import random
import signal
import string
import logging
import argparse
from dataclasses import dataclass
from typing import List, Dict, Any
from multiprocessing import Process, Queue, cpu_count, current_process
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed


# ============================================================
# Configuration Section
# ============================================================

@dataclass
class Config:
    total_records: int = 10000
    batch_size: int = 500
    cpu_workers: int = cpu_count()
    io_workers: int = 8
    output_file: str = "output.json"
    enable_benchmark: bool = True


# ============================================================
# Logging Setup
# ============================================================

def setup_logger():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(processName)s | %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )


# ============================================================
# Utility Functions
# ============================================================

def generate_random_string(length=10):
    return ''.join(random.choices(string.ascii_letters, k=length))


def simulate_cpu_heavy_task(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    CPU-bound simulation:
    Perform heavy mathematical calculations
    """
    result = 0
    for i in range(1, 5000):
        result += math.sqrt(i * random.random())

    record["computed_value"] = result
    record["process_id"] = os.getpid()
    return record


def simulate_io_task(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    IO-bound simulation:
    Simulate API call / DB insert
    """
    time.sleep(0.01)
    record["io_status"] = "success"
    return record


# ============================================================
# Data Generator
# ============================================================

class DataGenerator:

    def __init__(self, total_records: int):
        self.total_records = total_records

    def generate(self) -> List[Dict[str, Any]]:
        data = []
        for i in range(self.total_records):
            data.append({
                "id": i,
                "name": generate_random_string(),
                "value": random.randint(1, 1000)
            })
        return data


# ============================================================
# Batch Processor
# ============================================================

class BatchProcessor:

    def __init__(self, config: Config):
        self.config = config

    def create_batches(self, data: List[Dict[str, Any]]) -> List[List[Dict]]:
        batches = []
        for i in range(0, len(data), self.config.batch_size):
            batches.append(data[i:i + self.config.batch_size])
        return batches


# ============================================================
# CPU Parallel Processing
# ============================================================

def cpu_worker(batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    processed = []
    for record in batch:
        processed.append(simulate_cpu_heavy_task(record))
    return processed


class CPUParallelExecutor:

    def __init__(self, workers: int):
        self.workers = workers

    def process(self, batches: List[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        results = []

        with ProcessPoolExecutor(max_workers=self.workers) as executor:
            futures = [executor.submit(cpu_worker, batch) for batch in batches]

            for future in as_completed(futures):
                try:
                    results.extend(future.result())
                except Exception as e:
                    logging.error(f"CPU processing error: {e}")

        return results


# ============================================================
# IO Parallel Processing
# ============================================================

class IOParallelExecutor:

    def __init__(self, workers: int):
        self.workers = workers

    def process(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        results = []

        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            futures = [executor.submit(simulate_io_task, record) for record in data]

            for future in as_completed(futures):
                try:
                    results.append(future.result())
                except Exception as e:
                    logging.error(f"IO processing error: {e}")

        return results


# ============================================================
# File Writer
# ============================================================

class FileWriter:

    def __init__(self, output_file: str):
        self.output_file = output_file

    def write(self, data: List[Dict[str, Any]]):
        with open(self.output_file, "w") as f:
            json.dump(data, f, indent=2)


# ============================================================
# Benchmarking Utility
# ============================================================

class Benchmark:

    def __init__(self):
        self.start_times = {}
        self.end_times = {}

    def start(self, name: str):
        self.start_times[name] = time.time()

    def stop(self, name: str):
        self.end_times[name] = time.time()

    def report(self):
        logging.info("===== Benchmark Report =====")
        for name in self.start_times:
            duration = self.end_times[name] - self.start_times[name]
            logging.info(f"{name}: {duration:.2f} seconds")


# ============================================================
# Graceful Shutdown
# ============================================================

class GracefulKiller:
    kill_now = False

    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        logging.warning("Shutdown signal received.")
        self.kill_now = True


# ============================================================
# Main Pipeline
# ============================================================

class DataPipeline:

    def __init__(self, config: Config):
        self.config = config
        self.generator = DataGenerator(config.total_records)
        self.batch_processor = BatchProcessor(config)
        self.cpu_executor = CPUParallelExecutor(config.cpu_workers)
        self.io_executor = IOParallelExecutor(config.io_workers)
        self.writer = FileWriter(config.output_file)
        self.benchmark = Benchmark()
        self.killer = GracefulKiller()

    def run(self):

        logging.info("Starting pipeline...")

        # Step 1: Generate Data
        self.benchmark.start("Data Generation")
        data = self.generator.generate()
        self.benchmark.stop("Data Generation")

        if self.killer.kill_now:
            return

        # Step 2: Create Batches
        self.benchmark.start("Batch Creation")
        batches = self.batch_processor.create_batches(data)
        self.benchmark.stop("Batch Creation")

        if self.killer.kill_now:
            return

        # Step 3: CPU Parallel Processing
        self.benchmark.start("CPU Processing")
        cpu_processed = self.cpu_executor.process(batches)
        self.benchmark.stop("CPU Processing")

        if self.killer.kill_now:
            return

        # Step 4: IO Parallel Processing
        self.benchmark.start("IO Processing")
        io_processed = self.io_executor.process(cpu_processed)
        self.benchmark.stop("IO Processing")

        if self.killer.kill_now:
            return

        # Step 5: Write to File
        self.benchmark.start("File Writing")
        self.writer.write(io_processed)
        self.benchmark.stop("File Writing")

        logging.info("Pipeline completed successfully.")

        if self.config.enable_benchmark:
            self.benchmark.report()


# ============================================================
# Argument Parser
# ============================================================

def parse_arguments():
    parser = argparse.ArgumentParser(description="Parallel Data Pipeline")

    parser.add_argument("--records", type=int, default=10000)
    parser.add_argument("--batch", type=int, default=500)
    parser.add_argument("--cpu", type=int, default=cpu_count())
    parser.add_argument("--io", type=int, default=8)
    parser.add_argument("--output", type=str, default="output.json")

    args = parser.parse_args()

    return Config(
        total_records=args.records,
        batch_size=args.batch,
        cpu_workers=args.cpu,
        io_workers=args.io,
        output_file=args.output
    )


# ============================================================
# Entry Point
# ============================================================

def main():
    setup_logger()
    config = parse_arguments()
    pipeline = DataPipeline(config)
    pipeline.run()


if __name__ == "__main__":
    main()
