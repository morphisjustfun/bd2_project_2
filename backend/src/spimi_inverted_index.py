import json
import math
import os
import shutil
import pandas as pd
import nltk


class SPIMIInvertedIndex:
    datafile_name: str
    output_dir: str
    block_size: int
    stop_words: set[str]
    stemmer = nltk.SnowballStemmer("english")

    def __init__(self, data_file_name: str, output_dir: str, block_size: int) -> None:
        # nltk.download('punkt')
        # nltk.download('stopwords')
        self.output_dir = output_dir
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        os.makedirs(self.output_dir)
        self.dataFileName = data_file_name
        self.blockSize = block_size
        self.stop_words = set(nltk.corpus.stopwords.words('english'))

    def build(self) -> None:
        data = open(self.dataFileName)
        current_block_count = 1
        current_line_count = 0
        lines = []
        for line in data:
            lines.append(line)
            current_line_count += 1
            if current_line_count == self.blockSize:
                inverted_index, words = self.make_block(lines)
                self.write_block_to_disk(inverted_index, words, current_block_count)
                current_block_count += 1
                current_line_count = 0
                lines.clear()
        if len(lines) > 0:
            inverted_index, words = self.make_block(lines)
            self.write_block_to_disk(inverted_index, words, current_block_count)
        lines.clear()
        print('done')

    def process_word(self, content: list[str]) -> tuple[str, list[str]]:
        target = content[1]
        id = content[0]
        content_tokenized = nltk.word_tokenize(target)
        content_stemmed = [self.stemmer.stem(word) for word in content_tokenized]
        content_filtered = [word for word in content_stemmed if word.isalpha() and word not in self.stop_words]
        return id, content_filtered

    def make_block(self, lines: list[str]) -> tuple[dict, set[str]]:
        inverted_index = {}
        data = [json.loads(obj) for obj in lines]
        df = pd.DataFrame(data)
        df.loc[:, 'id'] = df.loc[:, 'id'].astype(str)
        content: list[list[str]] = df.loc[:, ['id', 'abstract']].values
        content_filtered = list(map(self.process_word, content))
        total_words = set()
        for id, words in content_filtered:
            unique_words = set(words)
            for word in unique_words:
                if word in inverted_index:
                    inverted_index[word].append((id, words.count(word)))
                else:
                    inverted_index[word] = [(id, words.count(word))]
            total_words.update(unique_words)
        return inverted_index, total_words

    def write_block_to_disk(self, inverted_index: dict, words: set[str], block_number: int) -> None:
        words_sorted = sorted(words)
        output_file_path = f'{self.output_dir}/temp_{block_number}.feather'
        df = pd.DataFrame(columns=['word', 'posting_list'])
        for word in words_sorted:
            df = pd.concat([df, pd.DataFrame({'word': [word], 'posting_list': [inverted_index[word]]})])
        df.reset_index(drop=True, inplace=True)
        df['posting_list'] = df['posting_list'].apply(lambda x: json.dumps(x))
        df.to_feather(output_file_path)

    def read_bloc_from_disk(self, temp_index: int) -> pd.DataFrame:
        df = pd.read_feather(f'{self.output_dir}/temp_{temp_index}.feather')
        df.set_index('word', inplace=True)
        df['posting_list'] = df['posting_list'].apply(lambda x: json.loads(x))
        return df

    def merge_blocks(self):
        # merge without creating a file, instead, sort and store words in order in each file
        # merge blocks. Use BSBI algorithm
        # blocks = [glob.glob(f'{self.output_dir}/temp_{i}.feather') for i in range(1, 5)]
        # levels = math.log2(self.blockSize)
        # initial_simultaneous_blocks = 2
        pass


