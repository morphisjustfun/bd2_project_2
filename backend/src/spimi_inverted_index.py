import glob
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
        self.dataFileName = data_file_name
        self.blockSize = block_size
        self.stop_words = set(nltk.corpus.stopwords.words('english'))

    def clean(self):
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        os.makedirs(self.output_dir)

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

    @staticmethod
    def read_bloc_from_disk(file_path: str) -> pd.DataFrame:
        df = pd.read_feather(file_path)
        df.set_index('word', inplace=True)
        df['posting_list'] = df['posting_list'].apply(lambda x: json.loads(x))
        return df

    def merge_blocks(self):
        # merge without creating a file, instead, sort and store words in order in each file
        # merge blocks. Use BSBI algorithm
        # blocks = [glob.glob(f'{self.output_dir}/temp_{i}.feather') for i in range(1, 5)]
        # levels = math.log2(self.blockSize)

        # total_block_files are all files in the dist/ directory
        total_blocks_files = glob.glob(f'{self.output_dir}/*.feather')
        total_current_batch_size = 2

        while total_current_batch_size != len(total_blocks_files):
            # create folder dist/totals_current_batch_size
            output_folder = f'{self.output_dir}/{total_current_batch_size}'

            if os.path.exists(output_folder):
                shutil.rmtree(output_folder)
            os.makedirs(output_folder)

            remainder = len(total_blocks_files) % total_current_batch_size
            if remainder != 0:
                total_blocks_files.extend([total_blocks_files[-1]] * (total_current_batch_size - remainder))

            for i in range(0, len(total_blocks_files), total_current_batch_size):
                # in this point total_current_batch_size files output must be created
                first_group = total_blocks_files[i:i + total_current_batch_size // 2]
                # first_group is the first half of the total_current_batch_size files
                second_group = total_blocks_files[i + total_current_batch_size // 2:i + total_current_batch_size]
                # second_group is the second half of the total_current_batch_size files
                files_written = 0

                # bsbi algorithm
                while len(first_group) > 0 or len(second_group) > 0:
                    first_buffer: None | pd.DataFrame = None
                    second_buffer: None | pd.DataFrame = None
                    output_buffer: pd.DataFrame
                    if len(first_group) > 0:
                        first_buffer = self.read_bloc_from_disk(first_group[0])
                        output_buffer = first_buffer.iloc[0:0].copy()
                    if len(second_group) > 0:
                        second_buffer = self.read_bloc_from_disk(second_group[0])
                        output_buffer = second_buffer.iloc[0:0].copy()

                    while len(first_buffer) > 0 and len(second_buffer) > 0:
                        first_word = first_buffer.index[0]
                        second_word = second_buffer.index[0]

                        # first first_word is smaller than second_word, then insert first_word into output_buffer
                        # and remove first_word from first_buffer
                        # if first_word is equal to second_word, then merge the posting lists of first_word and second_word
                        # and insert the merged posting list into output_buffer and remove first_word and second_word from

                        if first_word < second_word:
                            output_buffer = output_buffer.append(first_buffer.iloc[0])
                            first_buffer.drop(first_word, inplace=True)
                        elif first_word > second_word:
                            output_buffer = output_buffer.append(second_buffer.iloc[0])
                            second_buffer.drop(second_word, inplace=True)
                        else:
                            # merge posting lists, the index of the posting list is the id of the document
                            # the value of the posting list is the frequency of the word in the document
                            merged_posting_list = first_buffer.loc[first_word, 'posting_list'] + \
                                                  second_buffer.loc[second_word, 'posting_list']
                            # append (index-first_word, value-merged_posting_list) to output_buffer
                            output_buffer = output_buffer.append(pd.DataFrame({'posting_list': [merged_posting_list]},
                                                                              index=[first_word]))
                            first_buffer.drop(first_word, inplace=True)
                            second_buffer.drop(second_word, inplace=True)
