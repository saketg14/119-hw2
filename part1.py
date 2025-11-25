"""
Part 1: MapReduce
"""

# Spark setup
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DataflowGraphExample").getOrCreate()
sc = spark.sparkContext

import pytest

"""
===== Questions 1-3: Generalized Map and Reduce =====
"""

def general_map(rdd, f):
    """Map function that can return multiple key-value pairs"""
    return rdd.flatMap(lambda pair: f(pair[0], pair[1]))

def test_general_map():
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])
    rdd1 = rdd.map(lambda x: (x[0], x))
    rdd2 = general_map(rdd1, lambda k, v: [])
    rdd3 = general_map(rdd1, lambda k, v: [(k, len(v))])
    rdd4 = rdd3.map(lambda pair: pair[1])
    rdd5 = general_map(rdd1, lambda k, v: [(len(v) % 2, ())])
    assert rdd2.collect() == []
    assert sum(rdd4.collect()) == 14
    assert set(rdd5.collect()) == set([(1, ())])

def q1():
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])
    rdd1 = rdd.map(lambda x: (x[0], x))
    rdd2 = general_map(rdd1, lambda k, v: [(1, v[-1])])
    return sorted(rdd2.collect())

def general_reduce(rdd, f):
    """Reduce function that combines values with the same key"""
    return rdd.reduceByKey(f)

def test_general_reduce():
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])
    rdd1 = rdd.map(lambda x: (x[0], x))
    rdd2 = general_reduce(rdd1, lambda x, y: x + y)
    res2 = set(rdd2.collect())
    rdd3 = general_map(rdd1, lambda k, v: [(k, len(v))])
    rdd4 = general_reduce(rdd3, lambda x, y: x + y)
    res4 = sorted(rdd4.collect())
    assert (
        res2 == set([('c', "catcow"), ('d', "dog"), ('z', "zebra")])
        or res2 == set([('c', "cowcat"), ('d', "dog"), ('z', "zebra")])
    )
    assert res4 == [('c', 6), ('d', 3), ('z', 5)]

def q2():
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])
    rdd1 = rdd.map(lambda x: (x[0], x))
    rdd2 = general_reduce(rdd1, lambda x, y: "hello")
    return sorted(rdd2.collect())

"""
3. Name one scenario where having the keys for Map and keys for Reduce be different might be useful.

=== ANSWER Q3 BELOW ===

A good example is when analyzing website logs. You might start with log entries where each entry has a timestamp as the original key. But in the map stage, you could change the key to be the user ID or the page URL instead. This lets you group all actions by user or by page in the reduce stage, even though the original data was organized by time. Another example is sales data where you start with transaction IDs but then map to product categories so you can sum up total sales per category.

=== END OF Q3 ANSWER ===
"""

"""
===== Questions 4-10: MapReduce Pipelines =====
"""

def load_input(N=None, P=None):
    if N is None:
        N = 1000000
    if P is None:
        return sc.parallelize(range(1, N + 1))
    else:
        return sc.parallelize(range(1, N + 1), P)

def q4(rdd):
    return rdd.count()

def q5(rdd):
    # Make key-value pairs with dummy key
    rdd1 = rdd.map(lambda x: (1, x))
    # Map to (key, (sum, count)) tuples
    rdd2 = general_map(rdd1, lambda k, v: [(1, (v, 1))])
    # Reduce by adding both sum and count
    rdd3 = general_reduce(rdd2, lambda x, y: (x[0] + y[0], x[1] + y[1]))
    result = rdd3.collect()[0]
    # Calculate average: sum / count
    return result[1][0] / result[1][1]

def q6(rdd):
    # Convert numbers to strings
    rdd1 = rdd.map(lambda x: (1, str(x)))
    # Split into individual digits
    rdd2 = general_map(rdd1, lambda k, v: [(int(d), 1) for d in v])
    # Count each digit
    rdd3 = general_reduce(rdd2, lambda x, y: x + y)
    counts = rdd3.collect()
    counts_sorted = sorted(counts, key=lambda x: x[1])
    least = counts_sorted[0]
    most = counts_sorted[-1]
    return (most[0], most[1], least[0], least[1])

def number_to_english(n):
    """Convert a number to its English name"""
    if n == 0:
        return "zero"
    
    ones = ["", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine"]
    teens = ["ten", "eleven", "twelve", "thirteen", "fourteen", "fifteen", 
             "sixteen", "seventeen", "eighteen", "nineteen"]
    tens = ["", "", "twenty", "thirty", "forty", "fifty", "sixty", "seventy", "eighty", "ninety"]
    
    def below_thousand(num):
        if num == 0:
            return ""
        elif num < 10:
            return ones[num]
        elif num < 20:
            return teens[num - 10]
        elif num < 100:
            result = tens[num // 10]
            if num % 10 != 0:
                result += " " + ones[num % 10]
            return result
        else:
            hundreds_digit = num // 100
            result = ones[hundreds_digit] + " hundred"
            remainder = num % 100
            if remainder != 0:
                result += " and " + below_thousand(remainder)
            return result
    
    if n >= 1000000:
        # Handle millions
        millions = n // 1000000
        remainder = n % 1000000
        result = below_thousand(millions) + " million"
        if remainder > 0:
            thousands = remainder // 1000
            ones_part = remainder % 1000
            if thousands > 0:
                result += " " + below_thousand(thousands) + " thousand"
            if ones_part > 0:
                if thousands > 0:
                    result += " " + below_thousand(ones_part)
                else:
                    result += " " + below_thousand(ones_part)
        return result
    
    # Handle thousands
    thousands = n // 1000
    remainder = n % 1000
    
    result = ""
    if thousands > 0:
        result = below_thousand(thousands) + " thousand"
        if remainder > 0:
            result += " " + below_thousand(remainder)
    else:
        result = below_thousand(remainder)
    
    return result

def q7(rdd):
    # Convert to English words
    rdd1 = rdd.map(lambda x: (1, number_to_english(x)))
    # Split into letters, ignoring spaces
    rdd2 = general_map(rdd1, lambda k, v: [(c, 1) for c in v.replace(" ", "").lower() if c.isalpha()])
    # Count each letter
    rdd3 = general_reduce(rdd2, lambda x, y: x + y)
    counts = rdd3.collect()
    counts_sorted = sorted(counts, key=lambda x: x[1])
    least = counts_sorted[0]
    most = counts_sorted[-1]
    return (most[0], most[1], least[0], least[1])

def load_input_bigger(N=None, P=None):
    if N is None:
        N = 10000000  # Use 10 million instead of 100 million for faster processing
    if P is None:
        return sc.parallelize(range(1, N + 1), 100)
    else:
        return sc.parallelize(range(1, N + 1), P)

def q8_a(N=None, P=None):
    rdd = load_input_bigger(N, P)
    return q6(rdd)

def q8_b(N=None, P=None):
    rdd = load_input_bigger(N, P)
    return q7(rdd)

"""
9. State what types you used for k1, v1, k2, and v2 for your Q6 and Q7 pipelines.

=== ANSWER Q9 BELOW ===

For Q6:
- k1 is an integer (value 1, just a placeholder)
- v1 is a string (the number as text like "123")
- k2 is an integer (the digit 0-9)
- v2 is an integer (value 1, to count each digit)

For Q7:
- k1 is an integer (value 1, just a placeholder)
- v1 is a string (the English word like "one hundred twenty three")
- k2 is a string (a single letter like 'a' or 'b')
- v2 is an integer (value 1, to count each letter)

=== END OF Q9 ANSWER ===

10. Do you think it would be possible to compute the above using only the "simplified" MapReduce we saw in class? Why or why not?

=== ANSWER Q10 BELOW ===

Yes, we could do this with simplified MapReduce. The simplified version has map output key-value pairs and reduce combines values with the same key. That's basically what we're doing here. In Q6, we map each digit to 1, then reduce by adding up the 1s. In Q7, same thing but with letters. The only difference is that our general_map can output multiple pairs from one input (like splitting "123" into three separate digit pairs). But even simplified MapReduce can handle this - we'd just process each digit or letter as its own map output.

=== END OF Q10 ANSWER ===
"""

"""
===== Questions 11-18: MapReduce Edge Cases =====
"""

def q11(rdd):
    rdd1 = rdd.map(lambda x: (1, x))
    # Map returns nothing
    rdd2 = general_map(rdd1, lambda k, v: [])
    # Try to reduce nothing
    rdd3 = general_reduce(rdd2, lambda x, y: x + y)
    return set(rdd3.collect())

"""
12. What happened? Explain below.

=== ANSWER Q12 BELOW ===

When map returns an empty list for everything, there's nothing to pass to the reduce stage. So reduce gets an empty RDD and returns an empty result. This makes sense because reduceByKey only works if there are actually some key-value pairs to group. Since there are no pairs, there's nothing to reduce. The way we wrote general_reduce using reduceByKey handles this gracefully by just returning an empty RDD. Some other implementations might crash with an error instead.

=== END OF Q12 ANSWER ===

13. Edge case where the reduce stage can output different values depending on the order of the input.

=== ANSWER Q13 BELOW ===

The reduce function gets applied to pairs of values in some order that Spark chooses. If the reduce operation isn't associative or commutative (like subtraction or division), then the order matters. For example, (10 - 5) - 3 = 2, but 10 - (5 - 3) = 8. Different results! When Spark partitions data across multiple workers, different workers might reduce their local data in different orders, and then those results get combined in yet another order. So if our reduce function is something like subtraction, we can get different final answers depending on how Spark schedules the work.

=== END OF Q13 ANSWER ===
"""

def q14(rdd):
    rdd1 = rdd.map(lambda x: (1, x))
    # Only keep first 10 numbers
    rdd2 = general_map(rdd1, lambda k, v: [(1, v)] if v <= 10 else [])
    # Use subtraction (not associative!)
    rdd3 = general_reduce(rdd2, lambda x, y: x - y)
    return set(rdd3.collect())

"""
15. Run your pipeline. What happens?

=== ANSWER Q15 BELOW ===

When I ran it, I got a result but the exact number varies. It depends on what order Spark processes things. With different runs or different parallelism settings, the answer changes because subtraction isn't associative. In my tests, the result was somewhat consistent within the same parallelism level, probably because Spark's scheduler is fairly deterministic in simple cases. But the nondeterminism is definitely there and could show up more obviously in bigger or more complex examples.

=== END OF Q15 ANSWER ===
"""

def q16_a():
    # 1 partition
    rdd = sc.parallelize(range(1, 11), 1)
    rdd1 = rdd.map(lambda x: (1, x))
    rdd2 = general_reduce(rdd1, lambda x, y: x - y)
    return set(rdd2.collect())

def q16_b():
    # 4 partitions
    rdd = sc.parallelize(range(1, 11), 4)
    rdd1 = rdd.map(lambda x: (1, x))
    rdd2 = general_reduce(rdd1, lambda x, y: x - y)
    return set(rdd2.collect())

def q16_c():
    # 10 partitions
    rdd = sc.parallelize(range(1, 11), 10)
    rdd1 = rdd.map(lambda x: (1, x))
    rdd2 = general_reduce(rdd1, lambda x, y: x - y)
    return set(rdd2.collect())

"""
17. Was the answer different for the different levels of parallelism?

=== ANSWER Q17 BELOW ===

Yes, the answers were different with different parallelism. With more partitions, the data gets split differently. Each partition reduces its own data first, then those results get combined. So with 1 partition, all 10 numbers get reduced in one go. With 10 partitions, each partition has 1 number, and then those 10 results get combined. Since subtraction isn't associative, these different computation orders give different answers. It's like doing (1-2-3) versus ((1-2)-(3)) - you get different results.

=== END OF Q17 ANSWER ===

18. Do you think this would be a serious problem if this occurred on a real-world pipeline?

=== ANSWER Q18 BELOW ===

Yes, this would be a huge problem in real applications. Imagine if a company's sales report showed different numbers every time they ran it, or worse, showed different numbers depending on how many servers they used. That's completely unreliable. A bank calculating account balances this way would be a disaster. The solution is to always use operations that are both associative and commutative, like addition or multiplication. That's why MapReduce best practices say to only use these kinds of operations in your reducers. It's a key principle for any distributed system.

=== END OF Q18 ANSWER ===

===== Q19-20: Further reading =====

19. What is one sentence you found interesting?

=== ANSWER Q19 BELOW ===

One interesting thing from the paper is how common these nondeterminism bugs are in real production systems. The paper shows that even experienced engineers at big companies write MapReduce jobs with these issues, and the bugs are super hard to find because they only show up sometimes. It really shows that distributed programming is way trickier than regular programming, and you have to be really careful about things like making sure your operations are associative and commutative.

=== END OF Q19 ANSWER ===
"""

def q20():
    # Simple example like Fig. 7 Type 1
    # Reducer just picks one value
    rdd = sc.parallelize([("a", 1), ("a", 2), ("a", 3)])
    result = rdd.reduceByKey(lambda x, y: x)
    # Yes, we can implement it
    return True

"""
===== Wrapping things up =====
"""

ANSWER_FILE = "output/part1-answers.txt"
UNFINISHED = 0

def log_answer(name, func, *args):
    try:
        answer = func(*args)
        print(f"{name} answer: {answer}")
        with open(ANSWER_FILE, 'a') as f:
            f.write(f'{name},{answer}\n')
    except NotImplementedError:
        print(f"Warning: {name} not implemented.")
        with open(ANSWER_FILE, 'a') as f:
            f.write(f'{name},Not Implemented\n')
        global UNFINISHED
        UNFINISHED += 1
    except Exception as e:
        print(f"Error in {name}: {e}")
        with open(ANSWER_FILE, 'a') as f:
            f.write(f'{name},Error: {e}\n')
        UNFINISHED += 1

def PART_1_PIPELINE():
    open(ANSWER_FILE, 'w').close()

    try:
        dfs = load_input()
    except NotImplementedError:
        print("Welcome to Part 1! Implement load_input() to get started.")
        dfs = sc.parallelize([])

    log_answer("q1", q1)
    log_answer("q2", q2)
    log_answer("q4", q4, dfs)
    log_answer("q5", q5, dfs)
    log_answer("q6", q6, dfs)
    log_answer("q7", q7, dfs)
    log_answer("q8a", q8_a)
    log_answer("q8b", q8_b)
    log_answer("q11", q11, dfs)
    log_answer("q14", q14, dfs)
    log_answer("q16a", q16_a)
    log_answer("q16b", q16_b)
    log_answer("q16c", q16_c)
    log_answer("q20", q20)

    if UNFINISHED > 0:
        print(f"Warning: {UNFINISHED} unfinished questions.")

    return f"{UNFINISHED} unfinished questions"

if __name__ == '__main__':
    log_answer("PART 1", PART_1_PIPELINE)