import json
import os
import shutil
import pandas as pd


class SPIMIInvertedIndex:
    datafile_name: str
    output_dir: str
    block_size: int

    def __init__(self, data_file_name: str, output_dir: str, block_size: int):
        self.output_dir = output_dir
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        os.makedirs(self.output_dir)
        self.dataFileName = data_file_name
        self.blockSize = block_size


    def build(self):
        data = open(self.dataFileName)
        current_block_count = 1
        current_line_count = 0
        lines = []
        for line in data:
            lines.append(line)
            current_line_count += 1
            if current_line_count == self.blockSize:
                block = self.make_block(lines)
                self.write_block_to_disk(block, current_block_count)
                current_block_count += 1
                current_line_count = 0
                lines.clear()
                # test
                break


    def make_block(self, lines: list[str]):
        data = [json.loads(obj) for obj in lines]
        df = pd.DataFrame(data)
        content = df.loc[:, 'abstract'].values
        print(content)
        block = {}
        for line in lines:
            line = line.strip()
            if line == "":
                continue
            words = line.split(" ")
            for word in words:
                if word in block:
                    block[word] += 1
                else:
                    block[word] = 1
        return block

    def write_block_to_disk(self, block: dict, block_number: int):
        return
        block_file_name = self.output_dir + "/" + str(block_number) + ".txt"
        block_file = open(block_file_name, "w")
        for key in block:
            block_file.write(key + " " + str(block[key]) + "\n")
        block_file.close()