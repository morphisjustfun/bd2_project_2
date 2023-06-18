import glob
import os
import shutil
import nltk
from multiprocessing import Pool, cpu_count

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
                                 args=(lines.copy(), current_block_count, self.output_dir, self.stemmer, self.stop_words))
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
