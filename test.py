import queue


q = queue.Queue()

q.put(1)
q.put([1,2,3,4,5])

print(q.get())
print(q.get())