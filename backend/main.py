from src.spimi_inverted_index import SPIMIInvertedIndex

if __name__ == "__main__":
    test = SPIMIInvertedIndex(data_file_name="data/data.json", output_dir="dist/", block_size=10000)
    test.build()