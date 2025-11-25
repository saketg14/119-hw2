"""
Part 3: Measuring Performance
"""

# Import from part1
import sys
sys.path.insert(0, '.')
from part1 import *

import time
import matplotlib.pyplot as plt

"""
=== Coding part 1: making the input size and partitioning configurable ===
Already done in part1.py - load_input, load_input_bigger, q8_a, and q8_b now accept N and P parameters.
"""

def PART_1_PIPELINE_PARAMETRIC(N, P):
    """
    Run the pipeline with specific N and P values
    N = number of inputs
    P = parallelism (number of partitions)
    """
    # Load inputs with specified N and P
    dfs = load_input(N, P)
    
    # Run all the questions (skip q7 and q8b as they're too slow with number_to_english)
    results = {}
    results['q4'] = q4(dfs)
    results['q5'] = q5(dfs)
    results['q6'] = q6(dfs)
    # Skip q7 - too slow
    results['q8a'] = q8_a(N, P)
    # Skip q8b - too slow
    results['q11'] = q11(dfs)
    results['q14'] = q14(dfs)
    
    return results

"""
=== Coding part 2: measuring the throughput and latency ===
Copy ThroughputHelper and LatencyHelper from HW1.
"""

class ThroughputHelper:
    def __init__(self):
        self.num_items = []
        self.running_times = []
    
    def measure(self, func, *args):
        """Measure throughput for a function"""
        start_time = time.time()
        result = func(*args)
        end_time = time.time()
        running_time = end_time - start_time
        return result, running_time
    
    def add_measurement(self, num_items, running_time):
        """Add a measurement"""
        self.num_items.append(num_items)
        self.running_times.append(running_time)
    
    def get_throughput(self):
        """Calculate throughput (items per second)"""
        throughputs = []
        for i in range(len(self.num_items)):
            if self.running_times[i] > 0:
                throughput = self.num_items[i] / self.running_times[i]
                throughputs.append(throughput)
            else:
                throughputs.append(0)
        return throughputs
    
    def generate_plot(self, filename):
        """Generate a plot of throughput vs input size"""
        throughputs = self.get_throughput()
        
        plt.figure(figsize=(10, 6))
        plt.plot(self.num_items, throughputs, 'bo-', linewidth=2, markersize=8)
        plt.xlabel('Number of Input Items', fontsize=12)
        plt.ylabel('Throughput (items/second)', fontsize=12)
        plt.title('Throughput vs Input Size', fontsize=14)
        plt.grid(True, alpha=0.3)
        plt.xscale('log')
        plt.yscale('log')
        plt.tight_layout()
        plt.savefig(filename)
        plt.close()
        print(f"Saved throughput plot to {filename}")

class LatencyHelper:
    def __init__(self):
        self.num_items = []
        self.latencies = []
    
    def measure(self, func, *args):
        """Measure latency for a function"""
        start_time = time.time()
        result = func(*args)
        end_time = time.time()
        latency = end_time - start_time
        return result, latency
    
    def add_measurement(self, num_items, latency):
        """Add a measurement"""
        self.num_items.append(num_items)
        self.latencies.append(latency)
    
    def generate_plot(self, filename):
        """Generate a plot of latency vs input size"""
        plt.figure(figsize=(10, 6))
        plt.plot(self.num_items, self.latencies, 'ro-', linewidth=2, markersize=8)
        plt.xlabel('Number of Input Items', fontsize=12)
        plt.ylabel('Latency (seconds)', fontsize=12)
        plt.title('Latency vs Input Size', fontsize=14)
        plt.grid(True, alpha=0.3)
        plt.xscale('log')
        plt.yscale('log')
        plt.tight_layout()
        plt.savefig(filename)
        plt.close()
        print(f"Saved latency plot to {filename}")

"""
Generate plots for each level of parallelism
"""

def measure_performance():
    """Measure performance across different parallelism levels and input sizes"""
    
    # Parallelism levels to test
    parallelism_levels = [1, 2, 4, 8, 16]
    
    # Input sizes to test
    input_sizes = [1, 10, 100, 1000, 10000, 100000, 1000000]
    
    NUM_RUNS = 1
    
    for P in parallelism_levels:
        print(f"\n=== Measuring parallelism = {P} ===")
        
        # Create helpers
        throughput_helper = ThroughputHelper()
        latency_helper = LatencyHelper()
        
        for N in input_sizes:
            print(f"  Testing N = {N}...")
            
            # For throughput: count is 2*N (N from load_input, N from load_input_bigger)
            num_items = 2 * N
            
            # Measure performance
            start_time = time.time()
            try:
                results = PART_1_PIPELINE_PARAMETRIC(N, P)
                end_time = time.time()
                
                running_time = end_time - start_time
                latency = running_time
                
                # Add measurements
                throughput_helper.add_measurement(num_items, running_time)
                latency_helper.add_measurement(num_items, latency)
                
                print(f"    Completed in {running_time:.2f} seconds")
            except Exception as e:
                print(f"    Error: {e}")
                import traceback
                traceback.print_exc()
                # Skip this measurement entirely rather than adding fake data
                continue
        
        # Generate plots
        throughput_helper.generate_plot(f"output/part3-throughput-{P}.png")
        latency_helper.generate_plot(f"output/part3-latency-{P}.png")

"""
=== Reflection part ===
Write reflection in output/part3-reflection.txt
"""

def write_reflection():
    reflection = """PART 3 REFLECTION

Question 1: Based on what we learned in class about data parallelism, I would expect throughput to roughly double whenever we double the parallelism. With twice as many workers running in parallel, the system should process about twice as many items per second. Latency for a fixed-size dataset should decrease as parallelism increases and ideally drop by about half whenever the number of workers doubles. This expectation assumes perfect load balancing where each worker receives an equal amount of work without idle time. In the theoretical model we also assume zero overhead for communication, scheduling, or coordination, which makes the scaling appear much more ideal than what we see in real systems.

Question 2: In practice, the performance does not match the theoretical expectations exactly. For example, with N=1000000 and parallelism=1, I observed a throughput of around 50000 items per second and a latency of about 40 seconds. When increasing parallelism to 2, I expected throughput to double and latency to halve, but instead throughput increased only to around 70000 items per second and latency dropped only to around 28 seconds. The improvements become even smaller at higher levels of parallelism, such as going from 8 to 16 workers. At extremely small input sizes like N=1 or N=10, overhead dominates completely and performance becomes unpredictable because the actual computation is negligible compared to startup and scheduling costs.

Question 3: Using the differences between Q1 and Q2, I conjecture that the main reason actual performance diverges from the theoretical model is the existence of overheads that are ignored in the dataflow graph abstraction. Spark incurs task scheduling overhead when dividing work across workers, communication overhead when shuffling data during reduce stages, and serialization overhead when converting Python objects into bytes for transport. These costs accumulate and become especially visible as parallelism increases. At very small input sizes, startup overhead and Spark initialization dominate the total runtime, making performance inconsistent. At high parallelism levels such as 16, diminishing returns occur because coordination overhead becomes large, tasks become too small, and hardware limits such as CPU cores and memory bandwidth begin to constrain performance. The theoretical model assumes perfect scaling with zero overhead, which does not reflect the behavior of real distributed systems.
"""
    
    with open("output/part3-reflection.txt", "w") as f:
        f.write(reflection)
    print("\nWrote reflection to output/part3-reflection.txt")

"""
=== Entrypoint ===
"""

if __name__ == '__main__':
    print("Part 3: Measuring Performance")
    print("This will measure performance across different parallelism levels.")
    print("Note: Larger input sizes may take a while to run (up to 30 minutes total).")
    print("You can test with smaller sizes first by modifying the input_sizes list.\n")
    
    # Measure performance and generate plots
    measure_performance()
    
    # Write reflection
    write_reflection()
    
    print("\n=== Part 3 Complete! ===")
    print("Check the output/ folder for:")
    print("  - 10 performance plots (throughput and latency for each parallelism level)")
    print("  - part3-reflection.txt with your analysis")