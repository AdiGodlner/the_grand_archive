import os
import time
import multiprocessing as mp
from tqdm import tqdm

from mappers import british_museum
from nqParser import parse_chunk_fast, parse_chunk
from mappers.british_museum import bm_mapper
# The orchestrator (Orbs, batching, timing)

SAMPLE_NQ_FILE = "/home/adi/Desktop/keep/code/git/repos/the_grand_archive/maybe_useful/bm_eval/sample.nq"
FULL_NQ_FILE = "/home/adi/Desktop/keep/code/git/repos/the_grand_archive/maybe_useful/bm_eval/eval_full.nq"
TAIL_NQ_FILE = "/home/adi/Desktop/keep/code/git/repos/the_grand_archive/maybe_useful/bm_eval/out.nq"
SET_OUTPUT = "/home/adi/Desktop/keep/code/git/repos/the_grand_archive/maybe_useful/bm_eval/subject_out.txt"
PREDICATE_OUTPUT = "/home/adi/Desktop/keep/code/git/repos/the_grand_archive/maybe_useful/bm_eval/pred_out.txt"
GRAPH_OUTPUT = "/home/adi/Desktop/keep/code/git/repos/the_grand_archive/maybe_useful/bm_eval/graph_out.txt"

DB_FILE = "bm_artifacts.db"


def multi_processing(filename, num_producers, chunk_size=1024 * 1024 * 64):

    offsets = divide_file_to_chunks(filename, chunk_size)
    num_chunks = len(offsets)
    chunk_queue = mp.Queue()
    result_queue = mp.Queue()

    for offset in offsets:
        chunk_queue.put(offset)

    # Add termination sentinels for producers
    for _ in range(num_producers):
        chunk_queue.put(None)

    consumer_proc = mp.Process(target=consumer_work, args=(result_queue, num_producers, num_chunks))
    consumer_proc.start()

    procs = []

    for _ in range(num_producers):
        p = mp.Process(target=producer_work, args=(filename, chunk_queue, result_queue, True))
        p.start()
        procs.append(p)

    for p in procs:
        p.join()
    consumer_proc.join()

    print(f"Done. Schema consolidated ")


def divide_file_to_chunks(filename, chunk_size=1024 * 1024 * 64):
    """
        Calculates (start, end) byte offsets for the entire file.
        each chunk ends at a valid point i.e. a \n
    """
    file_size = os.path.getsize(filename)
    chunks = []
    start_ptr = 0

    with open(filename, 'rb') as f:

        while start_ptr < file_size:

            end_ptr = start_ptr + chunk_size

            if end_ptr > file_size:
                chunks.append((start_ptr, file_size))
                break

            f.seek(end_ptr) # jump to reading head to end pointer
            buffer_size = 4096
            found_new_line = False
            new_end_of_chunk = 0

            while not found_new_line:
                # loop until you find a new line or an EOF
                absolute_position_in_file = f.tell()
                buf = f.read(buffer_size) # note f.read does move the head pointer

                if not buf:
                    # reached the EOF
                    found_new_line = True
                    new_end_of_chunk =  file_size
                else:

                    new_line_index = buf.find(10) # b'\n'[0] := 10
                    if new_line_index != -1:
                        # found a new line
                        new_end_of_chunk = absolute_position_in_file + new_line_index + 1
                        found_new_line = True

            chunks.append((start_ptr, new_end_of_chunk))
            start_ptr = new_end_of_chunk

    return chunks


def consumer_work(result_queue, num_producers, num_chunks):
    final_subjects = set()
    final_predicates = set()
    final_graphs = set()
    finished_producers = 0

    pbar = tqdm(total=num_chunks, desc="Parsing N-Quads", unit="chunk")

    while finished_producers < num_producers:
        data = result_queue.get()
        if data is None:
            finished_producers += 1
            continue

        subs, preds, graphs = data
        final_subjects.update(subs)
        final_predicates.update(preds)
        final_graphs.update(graphs)
        pbar.update(1)

    pbar.close()

    # Final Write-out
    with open(SET_OUTPUT, 'w') as f:
        f.write("=== UNIQUE SUBJECT PATTERNS ===\n")
        for s in sorted(final_subjects): f.write(f"{s}\n")
    with open(PREDICATE_OUTPUT, "w") as f:
        f.write("\n=== UNIQUE PREDICATES ===\n")
        for p in sorted(final_predicates): f.write(f"{p}\n")
    # with open(GRAPH_OUTPUT, "w") as f:
    #     f.write("\n=== UNIQUE GRAPHS ===\n")
    #     for g in sorted(final_graphs): f.write(f"{g}\n")


def producer_work(filename, chunk_queue, result_queue, fast = True):

    while True:
        chunk_info = chunk_queue.get()
        if chunk_info is None:
            result_queue.put(None)  # Signal this producer is done
            break

        start, end = chunk_info
        local_subjects = set()
        local_predicates = set()
        local_graphs = set()

        quads = []

        with open(filename, 'rb') as f:
            f.seek(start)
            chunk_bytes = f.read(end - start)

            # Use 'yield from' to pass quads up from the chunk parser
            if fast:
                quads = parse_chunk_fast(chunk_bytes)
            else:
                quads = parse_chunk(chunk_bytes)

        i = 0
        for quad in quads:
            subject , predicate, obj, graph = british_museum.bm_mapper(quad)
            local_subjects.add(subject)
            local_predicates.add(predicate)
            local_graphs.add(graph)


        # Send unique sets found in this chunk to the consumer
        result_queue.put((local_subjects, local_predicates, local_graphs))


def stream_quads(file_name, chunk_size=1024 * 1024 * 64, fast = False):

    offsets = divide_file_to_chunks(file_name, chunk_size)
    quads = []
    with open(file_name, 'rb') as f:
        for start, end in offsets:
            # Move the read head to the start of our verified chunk
            f.seek(start)
            # Read the bytes between start and end
            chunk_bytes = f.read(end - start)

            new_quads = parse_chunk_fast(chunk_bytes) if fast else parse_chunk(chunk_bytes)
            quads.append( new_quads )


def test(filename):
    # Usage (The "Consumer" side)
    chunk_size = 1024*1024*64
    num_producers = 8
    multi_processing(filename, num_producers, chunk_size)



start_time = time.perf_counter()
test(FULL_NQ_FILE)
end_time = time.perf_counter()
duration = end_time - start_time
print(f"Function took {duration:.6f} seconds")