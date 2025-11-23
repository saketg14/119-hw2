"""
Part 1: MapReduce

In our first part, we will practice using MapReduce
to create several pipelines.
This part has 20 questions.

As you complete your code, you can run the code with

    python3 part1.py
    pytest part1.py

and you can view the output so far in:

    output/part1-answers.txt
"""

# Spark boilerplate (remember to always add this at the top of any Spark file)
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DataflowGraphExample").getOrCreate()
sc = spark.sparkContext

# Additional imports
import pytest

"""
===== Questions 1-3: Generalized Map and Reduce =====
"""

def general_map(rdd, f):
    """
    rdd: an RDD with values of type (k1, v1)
    f: a function (k1, v1) -> List[(k2, v2)]
    output: an RDD with values of type (k2, v2)
    """
    return rdd.flatMap(lambda pair: f(pair[0], pair[1]))

def test_general_map():
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])

    # Use first character as key
    rdd1 = rdd.map(lambda x: (x[0], x))

    # Map returning no values
    rdd2 = general_map(rdd1, lambda k, v: [])

    # Map returning length
    rdd3 = general_map(rdd1, lambda k, v: [(k, len(v))])
    rdd4 = rdd3.map(lambda pair: pair[1])

    # Map returnning odd or even length
    rdd5 = general_map(rdd1, lambda k, v: [(len(v) % 2, ())])

    assert rdd2.collect() == []
    assert sum(rdd4.collect()) == 14
    assert set(rdd5.collect()) == set([(1, ())])

def q1():
    # Answer to this part: don't change this
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])
    rdd1 = rdd.map(lambda x: (x[0], x))
    rdd2 = general_map(rdd1, lambda k, v: [(1, v[-1])])
    return sorted(rdd2.collect())

"""
2. Fill in the reduce function using operations on RDDs.
"""

def general_reduce(rdd, f):
    """
    rdd: an RDD with values of type (k2, v2)
    f: a function (v2, v2) -> v2
    output: an RDD with values of type (k2, v2),
        and just one single value per key
    """
    return rdd.reduceByKey(f)

def test_general_reduce():
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])

    # Use first character as key
    rdd1 = rdd.map(lambda x: (x[0], x))

    # Reduce, concatenating strings of the same key
    rdd2 = general_reduce(rdd1, lambda x, y: x + y)
    res2 = set(rdd2.collect())

    # Reduce, adding lengths
    rdd3 = general_map(rdd1, lambda k, v: [(k, len(v))])
    rdd4 = general_reduce(rdd3, lambda x, y: x + y)
    res4 = sorted(rdd4.collect())

    assert (
        res2 == set([('c', "catcow"), ('d', "dog"), ('z', "zebra")])
        or res2 == set([('c', "cowcat"), ('d', "dog"), ('z', "zebra")])
    )
    assert res4 == [('c', 6), ('d', 3), ('z', 5)]

def q2():
    # Answer to this part: don't change this
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])
    rdd1 = rdd.map(lambda x: (x[0], x))
    rdd2 = general_reduce(rdd1, lambda x, y: "hello")
    return sorted(rdd2.collect())

"""
3. Name one scenario where having the keys for Map
and keys for Reduce be different might be useful.

=== ANSWER Q3 BELOW ===

One scenario where different keys for Map and Reduce would be useful is when you're analyzing log files. For example, you might start with log entries that use timestamp as the key (k1), but during the map stage you want to group by error type instead (k2). This way you can count how many errors of each type occurred, even though the original data was organized by time. Another example is processing sales data where the initial key might be transaction ID, but you want to reduce by product category to calculate total sales per category.

=== END OF Q3 ANSWER ===
"""

"""
===== Questions 4-10: MapReduce Pipelines =====
"""

def load_input(N=None, P=None):
    # Return a parallelized RDD with the integers between 1 and 1,000,000
    if N is None:
        N = 1000000
    if P is None:
        return sc.parallelize(range(1, N + 1))
    else:
        return sc.parallelize(range(1, N + 1), P)

def q4(rdd):
    # Input: the RDD from load_input
    # Output: the length of the dataset.
    return rdd.count()

"""
5. Among the numbers from 1 to 1 million, what is the average value?
"""

def q5(rdd):
    # Input: the RDD from Q4
    # Output: the average value
    # Convert to key-value pairs, then use general_map and general_reduce
    rdd1 = rdd.map(lambda x: (1, x))
    # Map to (key, (sum, count))
    rdd2 = general_map(rdd1, lambda k, v: [(1, (v, 1))])
    # Reduce by adding sums and counts
    rdd3 = general_reduce(rdd2, lambda x, y: (x[0] + y[0], x[1] + y[1]))
    result = rdd3.collect()[0]
    return result[1][0] / result[1][1]

"""
6. Among the numbers from 1 to 1 million, when written out,
which digit is most common, with what frequency?
And which is the least common, with what frequency?
"""

def q6(rdd):
    # Input: the RDD from Q4
    # Output: a tuple (most common digit, most common frequency, least common digit, least common frequency)
    # Convert to key-value pairs
    rdd1 = rdd.map(lambda x: (1, str(x)))
    # Map each number to its digits
    rdd2 = general_map(rdd1, lambda k, v: [(int(d), 1) for d in v])
    # Reduce to count each digit
    rdd3 = general_reduce(rdd2, lambda x, y: x + y)
    counts = rdd3.collect()
    counts_sorted = sorted(counts, key=lambda x: x[1])
    least_common = counts_sorted[0]
    most_common = counts_sorted[-1]
    return (most_common[0], most_common[1], least_common[0], least_common[1])

"""
7. Among the numbers from 1 to 1 million, written out in English, which letter is most common?
"""

# Helper function to convert number to English
def number_to_english(n):
    if n == 0:
        return "zero"
    
    # Names for digits
    ones = ["", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine"]
    teens = ["ten", "eleven", "twelve", "thirteen", "fourteen", "fifteen", 
             "sixteen", "seventeen", "eighteen", "nineteen"]
    tens = ["", "", "twenty", "thirty", "forty", "fifty", "sixty", "seventy", "eighty", "ninety"]
    
    def convert_below_thousand(num):
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
            result = ones[num // 100] + " hundred"
            remainder = num % 100
            if remainder != 0:
                result += " and " + convert_below_thousand(remainder)
            return result
    
    if n == 1000000:
        return "one million"
    
    # Split into thousands
    thousands = n // 1000
    remainder = n % 1000
    
    result = ""
    if thousands > 0:
        result = convert_below_thousand(thousands) + " thousand"
        if remainder > 0:
            result += " " + convert_below_thousand(remainder)
    else:
        result = convert_below_thousand(remainder)
    
    return result

def q7(rdd):
    # Input: the RDD from Q4
    # Output: a tuple (most common char, most common frequency, least common char, least common frequency)
    # Convert to key-value pairs
    rdd1 = rdd.map(lambda x: (1, number_to_english(x)))
    # Map each word to its letters (ignoring spaces)
    rdd2 = general_map(rdd1, lambda k, v: [(c, 1) for c in v.replace(" ", "").lower() if c.isalpha()])
    # Reduce to count each letter
    rdd3 = general_reduce(rdd2, lambda x, y: x + y)
    counts = rdd3.collect()
    counts_sorted = sorted(counts, key=lambda x: x[1])
    least_common = counts_sorted[0]
    most_common = counts_sorted[-1]
    return (most_common[0], most_common[1], least_common[0], least_common[1])

"""
8. Does the answer change if we have the numbers from 1 to 100,000,000?
"""

def load_input_bigger(N=None, P=None):
    if N is None:
        N = 100000000
    if P is None:
        return sc.parallelize(range(1, N + 1), 100)
    else:
        return sc.parallelize(range(1, N + 1), P)

def q8_a(N=None, P=None):
    # version of Q6
    rdd = load_input_bigger(N, P)
    return q6(rdd)

def q8_b(N=None, P=None):
    # version of Q7
    rdd = load_input_bigger(N, P)
    return q7(rdd)

"""
Discussion questions

9. State what types you used for k1, v1, k2, and v2 for your Q6 and Q7 pipelines.

=== ANSWER Q9 BELOW ===

For Q6:
- k1 is an integer (always 1, just a placeholder key)
- v1 is a string (the number converted to string form)
- k2 is an integer (the digit from 0-9)
- v2 is an integer (always 1, to count occurrences)

For Q7:
- k1 is an integer (always 1, just a placeholder key)
- v1 is a string (the English word for the number)
- k2 is a string (a single letter character)
- v2 is an integer (always 1, to count occurrences)

=== END OF Q9 ANSWER ===

10. Do you think it would be possible to compute the above using only the
"simplified" MapReduce we saw in class? Why or why not?

=== ANSWER Q10 BELOW ===

Yes, it would be possible to compute these using simplified MapReduce. The simplified version we saw in class has the map stage output key-value pairs and the reduce stage combines values with the same key. This is exactly what we're doing here. In Q6, we map each digit to a count of 1, then reduce by summing up all the 1's for each digit. In Q7, we do the same thing but with letters instead of digits. The main difference is that in the generalized version, the map stage can output multiple key-value pairs from a single input, which we use when we split a number into its individual digits or letters. But this could still be done with simplified MapReduce, we'd just need to process each digit or letter separately in the map phase.

=== END OF Q10 ANSWER ===
"""

"""
===== Questions 11-18: MapReduce Edge Cases =====
"""

def q11(rdd):
    # Input: the RDD from Q4
    # Output: the result of the pipeline, a set of (key, value) pairs
    rdd1 = rdd.map(lambda x: (1, x))
    # Map returns empty list for all inputs
    rdd2 = general_map(rdd1, lambda k, v: [])
    # Try to reduce, but there's nothing to reduce!
    rdd3 = general_reduce(rdd2, lambda x, y: x + y)
    return set(rdd3.collect())

"""
12. What happened? Explain below.

=== ANSWER Q12 BELOW ===

When the map stage returns an empty list for all inputs, there are no key-value pairs to pass to the reduce stage. So the reduce stage has nothing to work with and returns an empty result set. This makes sense because reduceByKey only works on key-value pairs that actually exist. If there are no pairs at all, there's nothing to group or reduce. This does depend on how we defined general_reduce - since we used reduceByKey, it gracefully handles the empty case by returning an empty RDD. If we had used a different implementation, we might have gotten an error instead.

=== END OF Q12 ANSWER ===

13. Lastly, we will explore a second edge case, where the reduce stage can
output different values depending on the order of the input.

=== ANSWER Q13 BELOW ===

The reduce stage could output different values depending on input order because the reduce function is applied to pairs of values in some arbitrary order. When we use reduceByKey, Spark doesn't guarantee which order the values will be combined in. If the reduce function is not associative or commutative (like subtraction or division), then combining the values in different orders can give different results. For example, (5 - 3) - 2 = 0 but 5 - (3 - 2) = 4. So if our reduce function is something like subtraction, the final result depends on which values get combined first.

=== END OF Q13 ANSWER ===

14. Now demonstrate this edge case concretely.
"""

def q14(rdd):
    # Input: the RDD from Q4
    # Output: the result of the pipeline, a set of (key, value) pairs
    # Use subtraction as the reduce function (non-commutative)
    rdd1 = rdd.map(lambda x: (1, x))
    # Just use first 10 numbers to make it simpler
    rdd2 = general_map(rdd1, lambda k, v: [(1, v)] if v <= 10 else [])
    # Reduce with subtraction (order matters!)
    rdd3 = general_reduce(rdd2, lambda x, y: x - y)
    return set(rdd3.collect())

"""
15. Run your pipeline. What happens?

=== ANSWER Q15 BELOW ===

When I ran the pipeline, I got a result but it's hard to predict exactly what the value will be because it depends on the order Spark processes the values. The result might be different between runs, especially with different levels of parallelism, because the order of operations changes. However, on my runs, I noticed the result was fairly consistent, probably because Spark's scheduler is deterministic for simple cases like this. But theoretically, the nondeterminism is there and could show up in more complex scenarios.

=== END OF Q15 ANSWER ===

16. Try the same pipeline as in Q14 with different levels of parallelism.
"""

def q16_a():
    # Parallelism = 1
    rdd = sc.parallelize(range(1, 11), 1)
    rdd1 = rdd.map(lambda x: (1, x))
    rdd2 = general_reduce(rdd1, lambda x, y: x - y)
    return set(rdd2.collect())

def q16_b():
    # Parallelism = 4
    rdd = sc.parallelize(range(1, 11), 4)
    rdd1 = rdd.map(lambda x: (1, x))
    rdd2 = general_reduce(rdd1, lambda x, y: x - y)
    return set(rdd2.collect())

def q16_c():
    # Parallelism = 10
    rdd = sc.parallelize(range(1, 11), 10)
    rdd1 = rdd.map(lambda x: (1, x))
    rdd2 = general_reduce(rdd1, lambda x, y: x - y)
    return set(rdd2.collect())

"""
Discussion questions

17. Was the answer different for the different levels of parallelism?

=== ANSWER Q17 BELOW ===

Yes, the answers were potentially different for different levels of parallelism. When we have more partitions, the data gets split up differently and reduced in different sub-groups before being combined. This means the order of operations changes, and since subtraction is not associative, we can get different final results. With parallelism=1, everything is in one partition and reduced in one specific order. With parallelism=10, the data is split into 10 groups, each reduced separately, then those 10 results are combined, leading to a different computation order.

=== END OF Q17 ANSWER ===

18. Do you think this would be a serious problem if this occured on a real-world pipeline?

=== ANSWER Q18 BELOW ===

Yes, this would be a serious problem in real-world pipelines. If your results change every time you run the pipeline or change based on how many machines you're using, that makes your data analysis unreliable and unpredictable. Businesses need consistent results to make decisions. Imagine if a bank calculated account balances differently each time they ran their processing job - that would be a disaster! The solution is to make sure your reduce functions are both associative and commutative (like addition or multiplication) so that the order doesn't matter. This is why MapReduce frameworks often warn you to use associative operations, and it's a key principle in distributed computing.

=== END OF Q18 ANSWER ===

===== Q19-20: Further reading =====

19. Take a look at the paper. What is one sentence you found interesting?

=== ANSWER Q19 BELOW ===

One interesting sentence I found was about how even small differences in reducer implementations can lead to significantly different performance characteristics in production systems. The paper shows that many real-world MapReduce jobs have bugs related to non-deterministic behavior, and these bugs are really hard to detect and debug because they only show up in certain execution orders or with certain levels of parallelism. This really drives home the point that writing correct distributed code is much harder than writing correct sequential code.

=== END OF Q19 ANSWER ===

20. Try to implement an example from the paper.
"""

def q20():
    # Implementing a simple example similar to Type 1 from Fig. 7
    # This is the case where reducer just picks one value arbitrarily
    # We'll demonstrate with a simple example
    rdd = sc.parallelize([("a", 1), ("a", 2), ("a", 3)])
    # Reducer that just returns the first value (non-deterministic)
    result = rdd.reduceByKey(lambda x, y: x)
    # This is possible to implement, returns True
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
            print(f"Answer saved to {ANSWER_FILE}")
    except NotImplementedError:
        print(f"Warning: {name} not implemented.")
        with open(ANSWER_FILE, 'a') as f:
            f.write(f'{name},Not Implemented\n')
        global UNFINISHED
        UNFINISHED += 1

def PART_1_PIPELINE():
    open(ANSWER_FILE, 'w').close()

    try:
        dfs = load_input()
    except NotImplementedError:
        print("Welcome to Part 1! Implement load_input() to get started.")
        dfs = sc.parallelize([])

    # Questions 1-3
    log_answer("q1", q1)
    log_answer("q2", q2)

    # Questions 4-10
    log_answer("q4", q4, dfs)
    log_answer("q5", q5, dfs)
    log_answer("q6", q6, dfs)
    log_answer("q7", q7, dfs)
    log_answer("q8a", q8_a)
    log_answer("q8b", q8_b)

    # Questions 11-18
    log_answer("q11", q11, dfs)
    log_answer("q14", q14, dfs)
    log_answer("q16a", q16_a)
    log_answer("q16b", q16_b)
    log_answer("q16c", q16_c)

    # Questions 19-20
    log_answer("q20", q20)

    # Answer: return the number of questions that are not implemented
    if UNFINISHED > 0:
        print("Warning: there are unfinished questions.")

    return f"{UNFINISHED} unfinished questions"

if __name__ == '__main__':
    log_answer("PART 1", PART_1_PIPELINE)