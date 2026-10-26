import os
import re
import threading
from collections import defaultdict


REDUCER_COUNT = 2


def load_text(path):
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def map_file(path):
    content = load_text(path)
    words = re.findall(r"\b\w+\b", content.lower())

    return [(word, 1) for word in words]


def execute_mappers(input_files):
    mapped_data = [None] * len(input_files)
    workers = []

    def process_file(position, path):
        mapped_data[position] = map_file(path)

    for position, path in enumerate(input_files):
        worker = threading.Thread(
            target=process_file,
            args=(position, path)
        )
        workers.append(worker)
        worker.start()

    for worker in workers:
        worker.join()

    return mapped_data


def combine_results(mapped_data):
    combined_data = []

    for file_pairs in mapped_data:
        word_counts = defaultdict(int)

        for word, value in file_pairs:
            word_counts[word] += value

        combined_data.append(list(word_counts.items()))

    return combined_data


def get_reducer_id(word):
    character_sum = sum(ord(letter) for letter in word)
    return character_sum % REDUCER_COUNT


def distribute_data(combined_data):
    reducer_data = [[] for _ in range(REDUCER_COUNT)]

    for file_pairs in combined_data:
        for word, count in file_pairs:
            destination = get_reducer_id(word)
            reducer_data[destination].append((word, count))

    return reducer_data


def group_and_sort(reducer_data):
    word_groups = defaultdict(list)

    for word, count in reducer_data:
        word_groups[word].append(count)

    ordered_groups = {}

    for word in sorted(word_groups.keys()):
        ordered_groups[word] = word_groups[word]

    return ordered_groups


def save_reduced_data(grouped_data, destination):
    with open(destination, "w", encoding="utf-8") as f:
        for word, counts in grouped_data.items():
            word_total = sum(counts)
            f.write(f"{word}\t{word_total}\n")


def run_mapreduce(source_folder, destination_folder):
    os.makedirs(destination_folder, exist_ok=True)

    input_files = []

    for filename in sorted(os.listdir(source_folder)):
        if filename.endswith(".txt") and filename != "expected_word_count.txt":
            input_files.append(os.path.join(source_folder, filename))

    if len(input_files) == 0:
        raise FileNotFoundError(
            f"No input text files found in {source_folder}"
        )

    print("Number of input splits:", len(input_files))

    mapped_data = execute_mappers(input_files)

    combined_data = combine_results(mapped_data)

    reducer_data = distribute_data(combined_data)

    grouped_data = []

    for partition_data in reducer_data:
        grouped_data.append(group_and_sort(partition_data))


    reducer_threads = []

    for reducer_index, groups in enumerate(grouped_data):
        output_path = os.path.join(
            destination_folder,
            f"output{reducer_index}.txt"
        )

        thread = threading.Thread(
            target=save_reduced_data,
            args=(groups, output_path)
        )

        reducer_threads.append(thread)
        thread.start()

    for thread in reducer_threads:
        thread.join()

    print("Output files: output0.txt, output1.txt")


if __name__ == "__main__":
    current_folder = os.path.dirname(os.path.abspath(__file__))

    input_directory = os.path.join(
        current_folder, "datasets", "text-files"
    )

    output_directory = os.path.join(current_folder, "output")

    run_mapreduce(input_directory, output_directory)
