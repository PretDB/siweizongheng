#!/usr/bin/env python3
import random
import sys


meta_rec_len = int(sys.argv[1])

with open('data.csv', 'w') as f:
    f.write(f'{meta_rec_len}\n')
    for i in range(meta_rec_len):
        f.write(f'{random.choice([1000, 2000, 3000, 500, 1100, 6000])},{random.randrange(100)}\n')
        pass
    pass
