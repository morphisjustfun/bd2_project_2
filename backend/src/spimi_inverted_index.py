import glob
import json
import os
import shutil
import nltk
from multiprocessing import Pool, cpu_count

import numpy as np
import pandas as pd

from src.build_utils import process_lines
from src.external_sort_utils import merge


class SPIMIInvertedIndex:
    datafile_name: str
    output_dir: str
    block_size: int
    stop_words: set[str]
    stemmer = nltk.SnowballStemmer("english")

    def __init__(self, data_file_name: str, output_dir: str, block_size: int, language: str = 'english') -> None:
        # nltk.download('punkt')
        # nltk.download('stopwords')
        self.output_dir = output_dir
        self.dataFileName = data_file_name
        self.blockSize = block_size
        self.stop_words = set(nltk.corpus.stopwords.words(language))

    def clean(self):
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        os.makedirs(self.output_dir)

    def build(self) -> None:
        data = open(self.dataFileName)
        current_block_count = 1
        current_line_count = 0
        lines = []
        if not os.path.exists(f'{self.output_dir}/1/'):
            os.makedirs(f'{self.output_dir}/1/')
        pool = Pool(processes=cpu_count())
        for line in data:
            lines.append(line)
            current_line_count += 1
            if current_line_count == self.blockSize:
                pool.apply_async(process_lines,
                                 args=(
                                     lines.copy(), current_block_count, self.output_dir, self.stemmer, self.stop_words))
                current_block_count += 1
                current_line_count = 0
                lines.clear()
        if len(lines) > 0:
            pool.apply_async(process_lines,
                             args=(lines.copy(), current_block_count, self.output_dir, self.stemmer, self.stop_words))
        pool.close()
        pool.join()
        lines.clear()

    def merge_blocks(self):
        current_batch_size = 2
        total_files = len(glob.glob(f'{self.output_dir}/1/*.feather'))
        total_files_nearest_2n = 2 ** (total_files - 1).bit_length()
        while current_batch_size <= total_files_nearest_2n:
            output_dir = f'{self.output_dir}{current_batch_size}/'
            input_dir = f'{self.output_dir}{current_batch_size // 2}/'
            current_files = glob.glob(f'{input_dir}/*.feather')
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            current_files.sort(key=lambda x: int(x.split('/')[-1].split('_')[-1].split('.')[0]))
            index = 1
            pool = Pool(processes=cpu_count())
            for i in range(0, total_files, current_batch_size):
                first_group = current_files[i:i + current_batch_size // 2]
                second_group = current_files[i + current_batch_size // 2:i + current_batch_size]
                # merge(first_group, second_group, output_dir, index)
                pool.apply_async(merge, args=(first_group, second_group, output_dir, index))
                index = index + current_batch_size
            pool.close()
            pool.join()
            current_batch_size *= 2

    def preprocess_dist(self):
        maximum_folder = max([int(folder) for folder in os.listdir(self.output_dir)])
        total_files = len(glob.glob(f'{self.output_dir}{maximum_folder}/*.feather'))
        if not os.path.exists('processed/'):
            os.makedirs('processed/')
        for i in range(total_files):
            if pd.read_feather(f'{self.output_dir}{maximum_folder}/temp_{i + 1}.feather').empty:
                break
            shutil.copyfile(f'{self.output_dir}{maximum_folder}/temp_{i + 1}.feather', f'processed/{i + 1}.feather')

    def find_inverted_index_record(self, word: str, total_files: int) -> pd.DataFrame:
        # as files are ordered by word, we can use binary search, no file is empty
        left = 1
        right = total_files
        while left <= right:
            mid = (left + right) // 2
            data = pd.read_feather(f'processed/{mid}.feather')
            filter = data[data['word'] == word].copy()
            if filter.empty:
                if word < data['word'].iloc[0]:
                    right = mid - 1
                else:
                    left = mid + 1
            else:
                filter['posting_list'] = filter['posting_list'].apply(lambda x: json.loads(x))
                return filter
        # not found
        return pd.DataFrame(columns=['word', 'posting_list'])

    def query(self, query: str, k: int):
        scores = []
        query_vector = []  # tf # idf
        documents_vector = {}
        inverted_index_records = {}
        idfs = {}
        word_dictionary_posting_list = {}

        queryStream = nltk.word_tokenize(query, language='english')
        queryStream = [self.stemmer.stem(word) for word in queryStream]
        queryStream = [word for word in queryStream if word.isalpha() and word not in self.stop_words]
        total_files_preprocessed = len(glob.glob(f'processed/*.feather'))
        data = open(self.dataFileName)
        total_documents = 0
        for _ in data:
            total_documents += 1

        # FIND inverted index records
        for word in queryStream:
            inverted_index_record = self.find_inverted_index_record(word, total_files_preprocessed)
            inverted_index_records[word] = inverted_index_record

        # QUERY VECTOR
        for word in queryStream:
            inverted_index_record = inverted_index_records[word]
            if inverted_index_record.empty:
                query_vector.append(0)
                continue
            tf = queryStream.count(word)
            idf = np.log10(total_documents / len(inverted_index_record['posting_list'].iloc[0]))
            idfs[word] = idf
            query_vector.append(tf * idf)

        # normalize query vector
        query_vector = np.array(query_vector).astype(np.float64)
        query_vector = query_vector / np.linalg.norm(query_vector, 2)

        # DOCUMENT VECTOR
        documents_containing_query = set()
        for word in queryStream:
            inverted_index_record = inverted_index_records[word]
            if not inverted_index_record.empty:
                documents_containing_query.update(
                    list(map(lambda x: x[0], inverted_index_record['posting_list'].iloc[0])))

        for word in queryStream:
            dictionary_doc_id_tf = {}
            inverted_index_record = inverted_index_records[word]
            if inverted_index_record.empty:
                continue
            posting_list = inverted_index_record['posting_list'].iloc[0]
            for doc_id, tf in posting_list:
                dictionary_doc_id_tf[doc_id] = tf
            word_dictionary_posting_list[word] = dictionary_doc_id_tf

        for document in documents_containing_query:
            document_vector = []
            for word in queryStream:
                inverted_index_record = inverted_index_records[word]
                if inverted_index_record.empty:
                    document_vector.append(0)
                    continue
                doc_id_tf = word_dictionary_posting_list[word]
                if doc_id_tf:
                    tf = doc_id_tf.get(document, 0)
                    document_vector.append(tf * idfs[word])
                else:
                    document_vector.append(0)
            document_vector = np.array(document_vector).astype(np.float64)
            document_vector = document_vector / np.linalg.norm(document_vector, 2)
            documents_vector[document] = document_vector

        for document, vector in documents_vector.items():
            scores.append((document, np.dot(query_vector, vector)))

        # sort
        scores.sort(key=lambda x: x[1], reverse=True)
        selected = scores[:k]
        selected = list(map(lambda x: {'doc_id': x[0], 'score': x[1]}, selected))
        return selected
