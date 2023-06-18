import json

import nltk
import pandas as pd


def process_lines(lines: list[str], current_block_count: int, output_dir: str, stemmer, stop_words: set[str]) -> None:
    inverted_index, words = make_block(lines, stemmer, stop_words)
    write_block_to_disk(inverted_index, words, current_block_count, output_dir)


def process_word(content: list[str], stemmer, stop_words: set[str]) -> tuple[str, list[str]]:
    target = content[1]
    id = content[0]
    content_tokenized = nltk.word_tokenize(target)
    content_stemmed = [stemmer.stem(word) for word in content_tokenized]
    content_filtered = [word for word in content_stemmed if word.isalpha() and word not in stop_words]
    return id, content_filtered


def write_block_to_disk(inverted_index: dict, words: set[str], block_number: int, output_dir: str) -> None:
    words_sorted = sorted(words)
    output_file_path = f'{output_dir}/1/temp_{block_number}.feather'
    df = pd.DataFrame(columns=['word', 'posting_list'])
    for word in words_sorted:
        # df = df.append({'word': word, 'posting_list': inverted_index[word]}, ignore_index=True)
        # append will be deprecated in the future
        df = pd.concat([df, pd.DataFrame({'word': [word], 'posting_list': [inverted_index[word]]})])
    df.reset_index(inplace=True)
    df['posting_list'] = df['posting_list'].apply(lambda x: json.dumps(x))
    df.to_feather(output_file_path)


def make_block(lines: list[str], stemmer, stop_words: set[str]) -> tuple[dict, set[str]]:
    inverted_index = {}
    data = [json.loads(obj) for obj in lines]
    df = pd.DataFrame(data)
    df.loc[:, 'id'] = df.loc[:, 'id'].astype(str)
    content: list[list[str]] = df.loc[:, ['id', 'abstract']].values

    content_filtered = list(map(lambda x: process_word(x, stemmer, stop_words), content))
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
