ddd = {
    "aaa": 123,
    "bbb": 456,
}

l = [item for pair in ddd.items() for item in pair]
print(l)