import glob
import json
import os
import shutil
import pandas as pd
import nltk
from multiprocessing import Pool, cpu_count


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
            df = df.append({'word': word, 'posting_list': inverted_index[word]}, ignore_index=True)
        df.reset_index(inplace=True)
        df['posting_list'] = df['posting_list'].apply(lambda x: json.dumps(x))
        df.to_feather(output_file_path)

    @staticmethod
    def read_block_from_disk(file_path: str) -> pd.DataFrame:
        df = pd.read_feather(file_path)
        df.set_index('word', inplace=True)
        df['posting_list'] = df['posting_list'].apply(lambda x: json.loads(x))
        return df

    def write_pd_block_to_disk(self, inverted_index: pd.DataFrame, index: int, base_dir: str) -> None:
        output_file_path = f'{base_dir}temp_{index}.feather'
        inverted_index['posting_list'] = inverted_index['posting_list'].apply(lambda x: json.dumps(x))
        inverted_index.reset_index(inplace=True)
        inverted_index.rename(columns={'index': 'word'}, inplace=True)
        inverted_index.to_feather(output_file_path)
        inverted_index['posting_list'] = inverted_index['posting_list'].apply(lambda x: json.loads(x))
        inverted_index.set_index('word', inplace=True)

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
                # self.merge(first_group, second_group, output_dir, index)
                pool.apply_async(self.merge, args=(first_group, second_group, output_dir, index))
                index = index + current_batch_size
            pool.close()
            pool.join()
            current_batch_size *= 2

    def merge(self, first_group: list[str], second_group: list[str], output_dir: str, index: int) -> int:
        total_files_to_write = len(first_group) + len(second_group)
        total_length_per_output_buffer = self.read_block_from_disk(first_group[0]).shape[0]
        output_buffer = pd.DataFrame(columns=['word', 'posting_list'])
        output_buffer.set_index('word', inplace=True)
        first_buffer = pd.DataFrame(columns=['word', 'posting_list'])
        first_buffer.set_index('word', inplace=True)
        second_buffer = pd.DataFrame(columns=['word', 'posting_list'])
        second_buffer.set_index('word', inplace=True)
        # write all the files empty
        original_index = index
        for i in range(total_files_to_write):
            self.write_pd_block_to_disk(output_buffer, index, output_dir)
            index += 1
        index = original_index

        while len(first_group) > 0 or len(second_group) > 0:
            if len(first_group) > 0 and first_buffer.empty:
                first_buffer = self.read_block_from_disk(first_group[0])
                first_group.pop(0)

            if len(second_group) > 0 and second_buffer.empty:
                second_buffer = self.read_block_from_disk(second_group[0])
                second_group.pop(0)

            if first_buffer.empty and not second_buffer.empty:
                if total_files_to_write == 1:
                    for word in second_buffer.index:
                        if word in output_buffer.index:
                            posting_list = second_buffer.loc[word]['posting_list'] + output_buffer.loc[word][
                                'posting_list']
                            output_buffer.loc[word]['posting_list'] = posting_list
                        else:
                            output_buffer = pd.concat([output_buffer, second_buffer])
                    self.write_pd_block_to_disk(output_buffer, index, output_dir)
                    index += 1
                    return index

                if second_buffer.shape[0] + output_buffer.shape[0] > total_length_per_output_buffer:
                    to_add = second_buffer.iloc[0:total_length_per_output_buffer - output_buffer.shape[0], :]
                    for word in to_add.index:
                        if word in output_buffer.index:
                            posting_list = to_add.loc[word]['posting_list'] + output_buffer.loc[word]['posting_list']
                            output_buffer.loc[word]['posting_list'] = posting_list
                        else:
                            output_buffer = pd.concat([output_buffer, to_add])
                    second_buffer.drop(second_buffer.index[0:total_length_per_output_buffer - output_buffer.shape[0]],
                                       inplace=True)
                else:
                    for word in second_buffer.index:
                        if word in output_buffer.index:
                            posting_list = second_buffer.loc[word]['posting_list'] + output_buffer.loc[word][
                                'posting_list']
                            output_buffer.loc[word]['posting_list'] = posting_list
                        else:
                            output_buffer = pd.concat([output_buffer, second_buffer])
                    second_buffer = pd.DataFrame(columns=['word', 'posting_list'])
                    second_buffer.set_index('word', inplace=True)

                if output_buffer.shape[0] >= total_length_per_output_buffer and total_files_to_write > 1:
                    self.write_pd_block_to_disk(output_buffer, index, output_dir)
                    index += 1
                    output_buffer = pd.DataFrame(columns=['word', 'posting_list'])
                    output_buffer.set_index('word', inplace=True)
                    total_files_to_write -= 1

            if not first_buffer.empty and second_buffer.empty:
                if total_files_to_write == 1:
                    for word in first_buffer.index:
                        if word in output_buffer.index:
                            posting_list = first_buffer.loc[word]['posting_list'] + output_buffer.loc[word][
                                'posting_list']
                            output_buffer.loc[word]['posting_list'] = posting_list
                        else:
                            output_buffer = pd.concat([output_buffer, first_buffer])
                    self.write_pd_block_to_disk(output_buffer, index, output_dir)
                    index += 1
                    return index

                if first_buffer.shape[0] + output_buffer.shape[0] > total_length_per_output_buffer:
                    to_add = first_buffer.iloc[0:total_length_per_output_buffer - output_buffer.shape[0], :]
                    for word in to_add.index:
                        if word in output_buffer.index:
                            posting_list = to_add.loc[word]['posting_list'] + output_buffer.loc[word]['posting_list']
                            output_buffer.loc[word]['posting_list'] = posting_list
                        else:
                            output_buffer = pd.concat([output_buffer, to_add])
                    first_buffer.drop(first_buffer.index[0:total_length_per_output_buffer - output_buffer.shape[0]],
                                      inplace=True)
                else:
                    for word in first_buffer.index:
                        if word in output_buffer.index:
                            posting_list = first_buffer.loc[word]['posting_list'] + output_buffer.loc[word][
                                'posting_list']
                            output_buffer.loc[word]['posting_list'] = posting_list
                        else:
                            output_buffer = pd.concat([output_buffer, first_buffer])
                    first_buffer = pd.DataFrame(columns=['word', 'posting_list'])
                    first_buffer.set_index('word', inplace=True)

                if output_buffer.shape[0] >= total_length_per_output_buffer and total_files_to_write > 1:
                    self.write_pd_block_to_disk(output_buffer, index, output_dir)
                    index += 1
                    output_buffer = pd.DataFrame(columns=['word', 'posting_list'])
                    output_buffer.set_index('word', inplace=True)
                    total_files_to_write -= 1

            while not first_buffer.empty and not second_buffer.empty:
                first_word = first_buffer.index[0]
                second_word = second_buffer.index[0]
                if first_word < second_word:
                    output_buffer = pd.concat([output_buffer, first_buffer.iloc[0:1, :]])
                    first_buffer.drop(first_word, inplace=True)
                elif first_word > second_word:
                    output_buffer = pd.concat([output_buffer, second_buffer.iloc[0:1, :]])
                    second_buffer.drop(second_word, inplace=True)
                else:
                    posting_list = first_buffer.loc[first_word, 'posting_list'] + second_buffer.loc[
                        second_word, 'posting_list']
                    combined = pd.DataFrame({'word': [first_word],
                                             'posting_list': [posting_list]})
                    combined.set_index('word', inplace=True)
                    output_buffer = pd.concat([output_buffer, combined])
                    first_buffer.drop(first_word, inplace=True)
                    second_buffer.drop(second_word, inplace=True)

                if output_buffer.shape[0] >= total_length_per_output_buffer and total_files_to_write > 1:
                    self.write_pd_block_to_disk(output_buffer, index, output_dir)
                    index += 1
                    output_buffer = pd.DataFrame(columns=['word', 'posting_list'])
                    output_buffer.set_index('word', inplace=True)
                    total_files_to_write -= 1

        if total_files_to_write == 1:
            self.write_pd_block_to_disk(output_buffer, index, output_dir)
            index += 1

        return index
