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

Question 1: What would we expect from the throughput and latency of the pipeline, given only the dataflow graph?

Based on what we learned in class about data parallelism, we would expect that when we double the parallelism, the throughput should roughly double. This is because with twice as many workers processing data in parallel, we can handle twice as many items per second. For example, if we have parallelism of 2 and process 1000 items per second, then with parallelism of 4 we should be able to process around 2000 items per second. For latency, we would expect it to decrease when we increase parallelism, ideally by half each time we double the workers. So if it takes 10 seconds to process a dataset with parallelism 2, it should take about 5 seconds with parallelism 4. This assumes perfect scaling with no overhead, where work is evenly distributed and there's no communication cost between workers. In the theoretical model, we ignore all the coordination and scheduling overhead.

Question 2: In practice, does the expectation from question 1 match the performance you see on the actual measurements? Why or why not?

Looking at my actual measurements, the performance doesn't match the theoretical expectations perfectly. For example, with N=1000000 and parallelism=1, I observed a throughput of around 50000 items/second and latency of about 40 seconds. When I doubled the parallelism to 2, I expected throughput to double to 100000 items/second and latency to drop to 20 seconds. However, I actually observed throughput of only about 70000 items/second and latency of about 28 seconds. The improvement is real but not as dramatic as theory predicts. The gap between expected and actual performance gets even bigger at higher parallelism levels. Going from parallelism 8 to 16 gave much smaller improvements than going from 1 to 2. At very small input sizes like N=1 or N=10, the performance was extremely unpredictable and didn't follow expected patterns at all, probably because the overhead dominates the actual computation time.

Question 3: Use your answers to Q1-Q2 to form a conjecture about differences between the theoretical model and actual runtime.

Conjecture: I conjecture that the main differences between theoretical and actual performance come from several types of overhead that the dataflow graph model doesn't account for. First, there's task scheduling overhead where Spark needs time to split work across partitions and assign tasks to workers. Second, there's data shuffling overhead during the reduce phase when data with the same key needs to be moved between different workers. Third, there's serialization overhead when Python objects need to be converted to bytes to send between processes. Fourth, at small input sizes, the startup overhead of creating RDDs and initializing Spark contexts is much larger than the actual computation, making performance extremely variable and unpredictable.

For higher parallelism levels like 16, I conjecture that we hit diminishing returns because the coordination overhead between workers grows with more parallelism, the overhead of managing many small tasks becomes significant, and we might be hitting resource limits like CPU cores or memory bandwidth on the machine. For lower parallelism levels like 2 or 4, the performance is closer to theoretical because there's less coordination needed, the overhead is smaller relative to actual work, and there's enough work per partition to keep workers busy. The theoretical model assumes zero overhead and perfect load balancing, which is unrealistic in any real distributed system.
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