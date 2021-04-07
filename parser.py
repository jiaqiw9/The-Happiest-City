from mpi4py import MPI
import re, time
import sys
import math

# Regex for Tweet text.
TEXT_REGEX = b'(?:"text":")(.*?)(?:")'

# Regex to find individual tweets.
TWEET_REGEX = b'id":.*?"text".*?".*?"(?:,).*"doc".*?,'

COORD_REGEX = b'(?:"coordinates":)\[(.*?),(.*?)\]'

MAX_BUFF = int((2**31 - 1) / 2)

def read_AFINN(filename):
    # read AFINN dictionary
    word_scores = {}
    with open(filename) as AFINN:
        for line in AFINN:
            line_split = line.rsplit(maxsplit = 1)
            word, score = line_split[0], int(line_split[1])
            word_scores[word] = score
    return word_scores

def setup_AFINN(rank, comm):
    if rank == 0:
        word_scores = read_AFINN("AFINN.txt")
    else:
        word_scores = None
    word_scores = comm.bcast(word_scores, root=0)
    return word_scores


def main(argv):
    file_name = argv[0]
    print(file_name)
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    word_scores = setup_AFINN(rank, comm)

    print("Doing work on processor {}\n".format(rank))
    print(word_scores['abandon'])
    file = MPI.File.Open(comm, file_name, MPI.MODE_RDONLY)
    file_size = MPI.File.Get_size(file)
    chunk_size = int(math.ceil(file_size/size))
    offset_index = chunk_size * rank
    n_buffers = int(math.ceil(chunk_size / MAX_BUFF))
    buff_size = int(math.ceil(chunk_size / n_buffers))
    buffer = bytearray(buff_size)
    file.Read_at_all(offset_index, buffer)
    docs = re.findall(TWEET_REGEX, buffer, re.I)
    for doc in docs:
        text = re.search(TEXT_REGEX, doc).group(1)
        decoded = text.decode("utf-8")
        coordinates = re.search(COORD_REGEX, doc)
        long = float(coordinates.group(1))
        lat = float(coordinates.group(2))
        print(decoded)
        print("Coordinates: ",long, lat)
    file.Close()


    # if rank == 0:
    #     # Master
    # else:
    #     # Slave

if __name__ == "__main__":
    main(sys.argv[1:])
