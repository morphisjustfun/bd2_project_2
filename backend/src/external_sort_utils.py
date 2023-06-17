import json

import pandas as pd


def write_pd_block_to_disk(inverted_index: pd.DataFrame, index: int, base_dir: str) -> None:
    output_file_path = f'{base_dir}temp_{index}.feather'
    inverted_index['posting_list'] = inverted_index['posting_list'].apply(lambda x: json.dumps(x))
    inverted_index.reset_index(inplace=True)
    inverted_index.rename(columns={'index': 'word'}, inplace=True)
    inverted_index.to_feather(output_file_path)
    inverted_index['posting_list'] = inverted_index['posting_list'].apply(lambda x: json.loads(x))
    inverted_index.set_index('word', inplace=True)


def read_block_from_disk(file_path: str) -> pd.DataFrame:
    df = pd.read_feather(file_path)
    df.set_index('word', inplace=True)
    df['posting_list'] = df['posting_list'].apply(lambda x: json.loads(x))
    return df


def merge(first_group: list[str], second_group: list[str], output_dir: str, index: int) -> int:
    total_files_to_write = len(first_group) + len(second_group)
    total_length_per_output_buffer = read_block_from_disk(first_group[0]).shape[0]
    output_buffer = pd.DataFrame(columns=['word', 'posting_list'])
    output_buffer.set_index('word', inplace=True)
    first_buffer = pd.DataFrame(columns=['word', 'posting_list'])
    first_buffer.set_index('word', inplace=True)
    second_buffer = pd.DataFrame(columns=['word', 'posting_list'])
    second_buffer.set_index('word', inplace=True)
    # write all the files empty
    original_index = index
    for i in range(total_files_to_write):
        write_pd_block_to_disk(output_buffer, index, output_dir)
        index += 1
    index = original_index

    while len(first_group) > 0 or len(second_group) > 0:
        if len(first_group) > 0 and first_buffer.empty:
            first_buffer = read_block_from_disk(first_group[0])
            first_group.pop(0)

        if len(second_group) > 0 and second_buffer.empty:
            second_buffer = read_block_from_disk(second_group[0])
            second_group.pop(0)

        if first_buffer.empty and not second_buffer.empty:
            if total_files_to_write == 1:
                for word in second_buffer.index:
                    if word in output_buffer.index:
                        posting_list = second_buffer.loc[word]['posting_list'] + output_buffer.loc[word][
                            'posting_list']
                        output_buffer.loc[word]['posting_list'] = posting_list
                    else:
                        output_buffer = pd.concat([output_buffer, second_buffer.loc[[word]]])
                write_pd_block_to_disk(output_buffer, index, output_dir)
                index += 1
                return index

            if second_buffer.shape[0] + output_buffer.shape[0] > total_length_per_output_buffer:
                to_add = second_buffer.iloc[0:total_length_per_output_buffer - output_buffer.shape[0], :]
                for word in to_add.index:
                    if word in output_buffer.index:
                        posting_list = to_add.loc[word]['posting_list'] + output_buffer.loc[word]['posting_list']
                        output_buffer.loc[word]['posting_list'] = posting_list
                    else:
                        output_buffer = pd.concat([output_buffer, to_add.loc[[word]]])
                second_buffer.drop(second_buffer.index[0:total_length_per_output_buffer - output_buffer.shape[0]],
                                   inplace=True)
            else:
                for word in second_buffer.index:
                    if word in output_buffer.index:
                        posting_list = second_buffer.loc[word]['posting_list'] + output_buffer.loc[word][
                            'posting_list']
                        output_buffer.loc[word]['posting_list'] = posting_list
                    else:
                        output_buffer = pd.concat([output_buffer, second_buffer.loc[[word]]])
                second_buffer = pd.DataFrame(columns=['word', 'posting_list'])
                second_buffer.set_index('word', inplace=True)

            if output_buffer.shape[0] >= total_length_per_output_buffer and total_files_to_write > 1:
                write_pd_block_to_disk(output_buffer, index, output_dir)
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
                        output_buffer = pd.concat([output_buffer, first_buffer.loc[[word]]])
                write_pd_block_to_disk(output_buffer, index, output_dir)
                index += 1
                return index

            if first_buffer.shape[0] + output_buffer.shape[0] > total_length_per_output_buffer:
                to_add = first_buffer.iloc[0:total_length_per_output_buffer - output_buffer.shape[0], :]
                for word in to_add.index:
                    if word in output_buffer.index:
                        posting_list = to_add.loc[word]['posting_list'] + output_buffer.loc[word]['posting_list']
                        output_buffer.loc[word]['posting_list'] = posting_list
                    else:
                        output_buffer = pd.concat([output_buffer, to_add.loc[[word]]])
                first_buffer.drop(first_buffer.index[0:total_length_per_output_buffer - output_buffer.shape[0]],
                                  inplace=True)
            else:
                for word in first_buffer.index:
                    if word in output_buffer.index:
                        posting_list = first_buffer.loc[word]['posting_list'] + output_buffer.loc[word][
                            'posting_list']
                        output_buffer.loc[word]['posting_list'] = posting_list
                    else:
                        output_buffer = pd.concat([output_buffer, first_buffer.loc[[word]]])
                first_buffer = pd.DataFrame(columns=['word', 'posting_list'])
                first_buffer.set_index('word', inplace=True)

            if output_buffer.shape[0] >= total_length_per_output_buffer and total_files_to_write > 1:
                write_pd_block_to_disk(output_buffer, index, output_dir)
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
                write_pd_block_to_disk(output_buffer, index, output_dir)
                index += 1
                output_buffer = pd.DataFrame(columns=['word', 'posting_list'])
                output_buffer.set_index('word', inplace=True)
                total_files_to_write -= 1

    if not first_buffer.empty:
        for word in first_buffer.index:
            if word in output_buffer.index:
                posting_list = first_buffer.loc[word]['posting_list'] + output_buffer.loc[word]['posting_list']
                output_buffer.loc[word]['posting_list'] = posting_list
            else:
                output_buffer = pd.concat([output_buffer, first_buffer.loc[[word]]])

    if not second_buffer.empty:
        for word in second_buffer.index:
            if word in output_buffer.index:
                posting_list = second_buffer.loc[word]['posting_list'] + output_buffer.loc[word]['posting_list']
                output_buffer.loc[word]['posting_list'] = posting_list
            else:
                output_buffer = pd.concat([output_buffer, second_buffer.loc[[word]]])

    write_pd_block_to_disk(output_buffer, index, output_dir)
    index += 1

    return index
