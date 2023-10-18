#!/usr/bin/env python
import sys

# Iterate through input lines from stdin
for line in sys.stdin:
    # Split the line into words
    words = line.strip().split()
    
    # Emit key-value pairs (word, 1) for each word in the line
    for word in words:
        print(f"{word}\t1")
