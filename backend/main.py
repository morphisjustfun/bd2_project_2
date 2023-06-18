import time

from src.spimi_inverted_index import SPIMIInvertedIndex
from multiprocessing import cpu_count

if __name__ == "__main__":
    test = SPIMIInvertedIndex(data_file_name="data/data.json", output_dir="dist/", block_size=50000)
    test.clean()
    begin = time.time()
    test.build()
    end = time.time()
    print("Cores", cpu_count())
    print("Time elapsed: ", end - begin, "s")
    print("Time elapsed: ", (end - begin) * 1000, "ms")
    test.merge_blocks()

