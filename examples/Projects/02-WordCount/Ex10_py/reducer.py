#!/usr/bin/env python
import sys

current_word = None
word_count = 0

# Iterate through input lines from stdin
for line in sys.stdin:
    # Split the line into a word and its count
    word, count = line.strip().split("\t", 1)
    count = int(count)
    
    # If the current word is different from the new word, emit the count for the previous word
    if current_word and current_word != word:
        print(f"{current_word}\t{word_count}")
        word_count = 0
    
    current_word = word
    word_count += count

# Emit the count for the last word
if current_word:
    print(f"{current_word}\t{word_count}")
