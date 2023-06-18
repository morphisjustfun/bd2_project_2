import time

from src.spimi_inverted_index import SPIMIInvertedIndex
from multiprocessing import cpu_count

if __name__ == "__main__":
    test = SPIMIInvertedIndex(data_file_name="data/data.json", output_dir="dist/", block_size=50000)
    # test.clean()
    # test.build()
    # test.merge_blocks()
    # test.preprocess_dist()
    print(test.query('game assessment using iot and machine learning', 5))

