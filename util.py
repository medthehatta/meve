def upper_triangle(lst):
    lst = list(lst)
    for i in range(len(lst)):
        for j in range(i, len(lst)):
            yield (lst[i], lst[j])


