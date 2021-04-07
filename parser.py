from mpi4py import MPI
import re, time
import sys
import math

# Constants for assigning a coordinate to a region on the GRID
X_MIN, X_MAX = 144.7, 145.45 
Y_MIN, Y_MAX = -37.5, -38.1
Y_ORDS = ['A','B','C','D']
X_ORDS = ['1','2','3','4','5']
INTERVAL = 0.15


# REGEX
# Regex for Tweet text.
TEXT_REGEX = b'(?:"text":")(.*?)(?:")'
# Regex to find individual tweets.
TWEET_REGEX = b'id":.*?"text".*?".*?"(?:,).*"doc".*?,'
# Regex to extract the coordinate from a tweet.
COORD_REGEX = b'(?:"coordinates":)\[(.*?),(.*?)\]'

# MAX buffer size
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

def master(comm, file_name):
    rank, size = comm.Get_rank(), comm.Get_size()
    return

def collect_results(comm):
    return

def parse_tweets(file_name, rank, comm):
    size = comm.Get_size()
    return


# Calculates the sentiment score of a tweet
def calculate_score(word_scores, tweet):
    score = 0
    for word in word_scores:
        if word in tweet:
            score += word_scores[word]
    return score

# Adds the score to the correct grid's overall score.
def process_score(score, lat, long, grid_scores):
    x_min, x_max = 144.7, 145.45
    y_min, y_max = -37.5, -38.1
    return

def get_grid_cell(x, y):
    if y > Y_MIN or y < Y_MAX or x < X_MIN or x > X_MAX or (y > -37.8 and x > 145.3) or (y < -37.95 and x < 145):
        print("out of bounds")
        return None
    else:
        y_index = math.floor(abs(y - Y_MIN) / INTERVAL)
        x_index = math.floor(abs(x - X_MIN) / INTERVAL)
        # Error
        if x_index > 4 or y_index > 3:
            print("An error occurred when finding the grid associated with the point ({},{})".format(y, x))
            return None
        else:
            name = Y_ORDS[y_index] + X_ORDS[x_index]
            return name

def main(argv, comm):
    file_name = argv[0]
    print(file_name)
    rank = comm.Get_rank()
    size = comm.Get_size()

    word_scores = setup_AFINN(rank, comm)

    print("Doing work on processor {}".format(rank))
    # print(word_scores['abandon'])

    file = MPI.File.Open(comm, file_name, MPI.MODE_RDONLY)
    file_size = MPI.File.Get_size(file)
    print("{} is {} bytes.".format(file_name, file_size))
    # Chunk of file to read
    chunk_size = int(math.ceil(file_size/size))
    chunk_offset = chunk_size * rank # This is where the chunk starts
    print("Using chunks of size {} to read in the .json. Chunk offset is {}".format(chunk_size, chunk_offset))
    # Prepare to read the chunk in n_buffers:
    n_buffers = int(math.ceil(chunk_size / MAX_BUFF))
    buffer_size = int(math.ceil(chunk_size / n_buffers))
    print("Number of buffers to use: {}. Buffer size: {}".format(n_buffers, buffer_size))

    for i in range(n_buffers):    
        buffer = bytearray(buffer_size)
        buffer_offset = chunk_offset +  (i * buffer_size)
        # Read the file into the buffer
        file.Read_at_all(buffer_offset, buffer)
        # Find all the tweets contained in the buffer:
        tweets = re.findall(TWEET_REGEX, buffer, re.I)
        # Process each tweet
        for tweet in tweets:
            # print("Processor {} is performing the work".format(rank))
            text = re.search(TEXT_REGEX, tweet).group(1)
            decoded = text.decode("utf-8")
            score = calculate_score(word_scores, decoded.lower())
            coordinates = re.search(COORD_REGEX, tweet)
            long = float(coordinates.group(1))
            lat = float(coordinates.group(2))
            cell = get_grid_cell(long, lat)
            print(decoded)
            print("Coordinates: ",long, lat, " Cell: ",cell ," Sentiment score: ", score)
    file.Close()


if __name__ == "__main__":
    file_name = sys.argv[1:]
    comm = MPI.COMM_WORLD

    main(sys.argv[1:], comm)

    # comm = MPI.COMM_WORLD
    # rank = comm.Get_rank()

    # if rank == 0:
    #     master(comm, file_name)