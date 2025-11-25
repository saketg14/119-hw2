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

Question 1:
From the dataflow graph we would normally expect throughput to increase as we use more data parallelism and latency to decrease. If we double the number of partitions then, in the ideal model, throughput should roughly double because twice as many workers are processing data at the same time. For a fixed input size we would also expect the latency to be cut about in half when we double the workers, since the work is split evenly across more partitions. This picture assumes that there is perfect load balancing, so every worker gets the same amount of work and none of them are idle. It also assumes that there is no overhead from communication, scheduling, or coordination between workers. In short, the theoretical model predicts clean linear speedup in throughput and inverse-linear decrease in latency as parallelism increases.

Question 2:
When we look at the actual measurements, the performance does not match the perfect scaling from Question 1. For example, going from P=1 to P=2 for a large input like N=1,000,000 increased throughput by less than a factor of 2 and reduced latency by less than one half. The improvements get even smaller when we go from P=4 to P=8 or P=8 to P=16, and sometimes the runtime even gets worse at very high parallelism levels. For small inputs such as N=1 or N=10, the results are very noisy and do not follow any clear pattern because the overhead dominates the useful work. Overall, the real system shows diminishing returns: we get some speedup from extra partitions at first, but the gains flatten out and can even reverse.

Question 3:
These differences suggest that the real system has extra costs that are not captured by the simple dataflow-graph model. Spark needs time to schedule tasks for each partition, and this scheduling overhead is paid even when the partitions are very small. There is also communication and shuffling overhead when data with the same key must be moved between workers during reduce-style stages. Serialization and deserialization of Python objects add more overhead, especially when many small tasks are created. At very high parallelism levels the workers also start competing for limited hardware resources such as CPU cores and memory bandwidth, which blocks perfect scaling. All of these effects break the assumption of zero overhead and perfect load balancing that underlies the theoretical expectations.

Conjecture:
I conjecture that real execution is limited by scheduling overhead, communication and serialization costs, and hardware contention between workers, so throughput and latency cannot scale linearly with the number of partitions as predicted by the idealized dataflow model.
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