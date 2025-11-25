def compute_min_radius(P, A, k):
    n = len(P)
    if n <= 1:
        return 0

    # Group cities by their percentage (20, 40, 60, 80, 100)
    groups = [[] for _ in range(5)]
    for i, pi in enumerate(P):
        groups[pi // 20 - 1].append(i)

    if A[-1] == A[0]:  # all cities at same spot
        return 0

    n_local = n
    A_local = A
    groups_local = groups

    def ok(q):  # check if radius q/60 works with k towers
        L = [0] * n_local  # leftmost city each tower can cover
        R = [0] * n_local  # rightmost city each tower can cover

        # Find how far right each tower can reach
        for gi in range(5):
            idxs = groups_local[gi]
            if not idxs:
                continue
            p_val = (gi + 1) * 20
            limit_mul = q * p_val
            j = idxs[0]
            for i in idxs:
                if j < i:
                    j = i
                Ai = A_local[i]
                while j + 1 < n_local and 6000 * (A_local[j + 1] - Ai) <= limit_mul:
                    j += 1
                R[i] = j

        # Find how far left each tower can reach
        for gi in range(5):
            idxs = groups_local[gi]
            if not idxs:
                continue
            p_val = (gi + 1) * 20
            limit_mul = q * p_val
            j = idxs[-1]
            for i in reversed(idxs):
                if j > i:
                    j = i
                Ai = A_local[i]
                while j - 1 >= 0 and 6000 * (Ai - A_local[j - 1]) <= limit_mul:
                    j -= 1
                L[i] = j

        # Group intervals by where they start
        start_at = [[] for _ in range(n_local)]
        for i in range(n_local):
            start_at[L[i]].append(R[i])

        # For each position, track the furthest we can reach
        r_cover = [0] * n_local
        cur_max = -1
        for x in range(n_local):
            for r in start_at[x]:
                if r > cur_max:
                    cur_max = r
            r_cover[x] = cur_max

        # Greedily jump as far as possible with each tower
        cur = 0
        used = 0
        while cur < n_local and used < k:
            if r_cover[cur] < cur:
                break
            cur = r_cover[cur] + 1
            used += 1

        return cur >= n_local

    # Binary search for minimum radius
    lo = 0
    hi = 6000 * (A[-1] - A[0])

    while lo < hi:
        mid = (lo + hi) // 2
        if ok(mid):
            hi = mid
        else:
            lo = mid + 1

    return lo


tc = int(input())
for _ in range(tc):
    n, k = map(int, input().split())
    P = list(map(int, input().split()))
    A = list(map(int, input().split()))
    print(compute_min_radius(P, A, k))