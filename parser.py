from mpi4py import MPI
import re, time
import sys
import math

# Regex for Tweet text.
TEXT_REGEX = b'(?:"text":")(.*?)(?:")'

# Regex to find individual doc.
DOC_REGEX = b'doc":.*?"text".*?".*?"(?:,).*"lang".*?,'

COORD_REGEX = b'(?:"coordinates":)\[(.*?),(.*?)\]'

MAX_BUFF = int((2**31 - 1) / 2)



def main(argv):
    file_name = argv[0]
    print(file_name)
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    file = MPI.File.Open(comm, file_name, MPI.MODE_RDONLY)
    file_size = MPI.File.Get_size(file)
    chunk_size = int(math.ceil(file_size/size))
    offset_index = chunk_size * rank
    n_buffers = int(math.ceil(chunk_size / MAX_BUFF))
    buff_size = int(math.ceil(chunk_size / n_buffers))
    buffer = bytearray(buff_size)
    file.Read_at_all(offset_index, buffer)
    docs = re.findall(DOC_REGEX, buffer, re.I)
    for doc in docs:
        text = re.search(TEXT_REGEX, doc).group(1)
        coordinates = re.search(COORD_REGEX, doc)
        lat = float(coordinates.group(1))
        long = float(coordinates.group(2))
        print(text, lat, long)
    file.Close()


    # if rank == 0:
    #     # Master
    # else:
    #     # Slave

if __name__ == "__main__":
    main(sys.argv[1:])
