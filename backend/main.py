import sys

from src.spimi_inverted_index import SPIMIInvertedIndex
from multiprocessing import cpu_count
import time

if __name__ == "__main__":
    query = sys.argv[1]
    k = int(sys.argv[2])
    test = SPIMIInvertedIndex(data_file_name="data/data.json", output_dir="dist/", block_size=50000)
    # test.clean()
    # test.build()
    # test.merge_blocks()
    # test.preprocess_dist()

    startPython = time.perf_counter()
    queryResult = test.query(query, k)
    endPython = time.perf_counter()

    timePythonMs = endPython - startPython
    resultPython = {
        'result': queryResult,
        'time': timePythonMs,
    }

    finalResult = {
        'python': resultPython,
        'postgreSQL': 2
    }

    print(resultPython)

