from mpi4py import MPI
import re
import time
import sys
import math

# Constants for assigning a coordinate to a region on the GRID
X_MIN, X_MAX = 144.7, 145.45
Y_MIN, Y_MAX = -37.5, -38.1
Y_ORDS = ['A', 'B', 'C', 'D']
X_ORDS = ['1', '2', '3', '4', '5']
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


def read_AFINN(filename: str) -> dict:
    '''
    Converts AFINN.txt into a dictionary mapping words to sentiment scores
    '''
    # read AFINN dictionary
    word_scores = {}
    with open(filename) as AFINN:
        for line in AFINN:
            line_split = line.rsplit(maxsplit=1)
            word, score = line_split[0], int(line_split[1])
            word_scores[word] = score
    return word_scores


def setup_AFINN(rank, comm):
    '''
    Sends the map of words to sentiment scores to all nodes
    '''
    if rank == 0:
        word_scores = read_AFINN("AFINN.txt")
    else:
        word_scores = None
    word_scores = comm.bcast(word_scores, root=0)
    return word_scores


def setup_grid_scores():
    grid_scores = {}
    keys = ["A1", "A2", "A3", "A4", 'B1', 'B2',
            'B3', 'B4', 'C1', 'C2', 'C3', 'C4',
            'C5', 'D3', 'D4', 'D5']
    for key in keys:
        grid_scores[key] = {'count': 0, 'score': 0}
    return grid_scores


def master(comm, file_name):
    '''
    Main function for the master processor
    '''
    rank, size = comm.Get_rank(), comm.Get_size()

    final_result = parse_tweets(file_name, rank, comm)
    print(final_result)
    return


def collect_results(comm):
    return

def slave(comm, file_name):
    rank, size = comm.Get_rank(), comm.Get_size()

    # Do work
    output = parse_tweets(file_name, rank, comm)
    print(output)
    return

def open_file(comm, fname):
    print("trying to open ", fname)
    file = MPI.File.Open(comm, fname, MPI.MODE_RDONLY)
    file_size = MPI.File.Get_size(file)
    return file, file_size


def parse_tweets(file_name: str, rank, comm):
    size = comm.Get_size()
    # Set-up
    word_scores = setup_AFINN(rank, comm)
    grid_scores = setup_grid_scores()
    print("Doing work on processor {}".format(rank))

    # Open the .json containing all tweets
    file, file_size = open_file(comm, file_name)
    print("{} is {} bytes.".format(file_name, file_size))

    # Chunk of file to read
    chunk_size = int(math.ceil(file_size/size))
    chunk_offset = chunk_size * rank  # This is where the chunk to be read starts

    print("Using chunks of size {} to read in the .json. Chunk offset is {}".format(
        chunk_size, chunk_offset))
    # Find how many buffers are needed to read the chunk in
    n_buffers = int(math.ceil(chunk_size / MAX_BUFF))
    buffer_size = int(math.ceil(chunk_size / n_buffers))
    print("Number of buffers to use: {}. Buffer size: {}".format(
        n_buffers, buffer_size))
    for i in range(n_buffers):
        buffer = bytearray(buffer_size)
        buffer_offset = chunk_offset + (i * buffer_size)
        # Read the file into the buffer
        file.Read_at_all(buffer_offset, buffer)
        # Find all the tweets contained in the buffer:
        tweets = re.findall(TWEET_REGEX, buffer, re.I)
        # Process each tweet
        for tweet in tweets:
            cell, score = parse_single_tweet(tweet, word_scores)
            process_score(score, cell, grid_scores)
            print(" Cell: ", cell, " Sentiment score: ", score)
    file.Close()
    return grid_scores


def parse_single_tweet(tweet, word_scores):
    '''
    Extracts the contents and co-ordinates of the tweet and parses the result
    '''
    text = re.search(TEXT_REGEX, tweet).group(1)
    decoded = text.decode("utf-8")
    score = calculate_score(word_scores, decoded.lower())
    coordinates = re.search(COORD_REGEX, tweet)
    long = float(coordinates.group(1))
    lat = float(coordinates.group(2))
    cell = get_grid_cell(long, lat)
    print(decoded)
    return cell, score


def calculate_score(word_scores: dict, tweet: re.Match) -> int:
    '''
    Calculates the sentiment score of a tweet
    '''
    score = 0
    for word in word_scores:
        if word in tweet:
            score += word_scores[word]
    return score


def process_score(score, grid_cell, grid_scores):
    '''
    Increments the count of tweets encounter in a given cell and also 
    modifies the sentiment score of the cell accordingly
    '''
    if grid_cell == "A5" or grid_cell == "B5" or grid_cell == "D1" or grid_cell == "D2":
        print("Error - we have an out of bounds cell being counted")
    grid_scores[grid_cell]['score'] += score
    grid_scores[grid_cell]['count'] += 1
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
            print(
                "An error occurred when finding the grid associated with the point ({},{})".format(y, x))
            return None
        else:
            name = Y_ORDS[y_index] + X_ORDS[x_index]
            return name


if __name__ == "__main__":
    file_name = sys.argv[1:]
    comm = MPI.COMM_WORLD
    fname = file_name[0]
    rank = comm.Get_rank()
    print(fname)
    if rank == 0:
        master(comm, fname)
    else:
        slave(comm, fname)