from mpi4py import MPI
import re, time, math
import aho, grid
from logger import Logger

# Regex for Tweet text.
TEXT_REGEX = b'(?:"text":")(.*?)(?:")'
# Regex to find individual tweets.
TWEET_REGEX = b'doc":.*?"text".*?".*?"(?:,).*"lang".*?,'
# Regex to extract the coordinate from a tweet.
COORD_REGEX = b'"geo".*?(?:"coordinates":)\[+(.*?),(.*?)\]+'

# MAX buffer size
MAX_BUFF = int((2**31 - 1) / 2)
# Initalize logger
logger = Logger("master")


def read_afinn(filename: str) -> dict:
    '''
    Converts AFINN.txt into a dictionary mapping words to sentiment scores
    '''
    # read AFINN dictionary
    word_scores = {}
    with open(filename, 'r') as AFINN:
        for line in AFINN:
            line_split = line.rsplit(maxsplit=1)
            word, score = line_split[0], int(line_split[1])
            word_scores[word] = score
    return word_scores


def setup_afinn(rank, comm):
    '''
    Sends the map of words to sentiment scores to all nodes
    '''
    if rank == 0:
        word_scores = read_afinn("AFINN.txt")
    else:
        word_scores = None
    word_scores = comm.bcast(word_scores, root=0)
    return word_scores


def setup_grid_scores():
    '''
    Builds a dictionary to store the sentiment score and number of tweets observed for each grid cell
    '''
    grid_scores = {}
    keys = ["A1", "A2", "A3", "A4", 'B1', 'B2',
            'B3', 'B4', 'C1', 'C2', 'C3', 'C4',
            'C5', 'D3', 'D4', 'D5']
    for key in keys:
        grid_scores[key] = {'count': 0, 'score': 0}
    return grid_scores


def run_main(comm, file_name):
    rank, size = comm.Get_rank(), comm.Get_size()
    output = parse_tweets(file_name, rank, comm, size)
    logger.log("Worker {} finished.".format(rank))
    output_list = comm.gather(output, root=0)
    if rank == 0: 
        if size != 1:
            logger.log("Master process completed. Now gathering return data from all slaves")
            output = gather_output(output_list)    
            logger.log("Succesfully combined output from all slaves")
        print_results(output)
    return

def gather_output(output_list):
    master_output = output_list[0]
    for i in range(1, len(output_list)):
        add_slave_output(output_list[i], master_output)
    return master_output

def add_slave_output(slave_output, slave_results):
    for cell in slave_output:
        slave_results[cell]['count'] += slave_output[cell]['count']
        slave_results[cell]['score'] += slave_output[cell]['score']


def open_file(comm, fname, rank):
    try:
        file = MPI.File.Open(comm, fname, MPI.MODE_RDONLY)
        file_size = MPI.File.Get_size(file)
        return file, file_size
    except BufferError:
        logger.log_error("Unable to open file at worker {}".format(rank))
        return None, 0


def which_chunk(file_size, size, rank):
    '''
    Determines the chunk of the file the worker is responsible for processing 
    '''
    chunk_size = int(math.ceil(file_size/size))
    chunk_offset = chunk_size * rank  # This is where the chunk to be read starts
    # Find how many buffers are needed to read the chunk in
    n_buffers = int(math.ceil(chunk_size / MAX_BUFF))
    return chunk_size, chunk_offset, n_buffers

def write_to_buffer(file, chunk_size, n_buffers, chunk_offset, buffer_index):
    buffer_size = int(math.ceil(chunk_size / n_buffers))
    buffer = bytearray(buffer_size)
    buffer_offset = chunk_offset + (buffer_index * buffer_size)
    file.Read_at_all(buffer_offset, buffer)
    return buffer

def parse_tweets(file_name: str, rank, comm, size):
    # Set-up the data-structures needed to process the json
    word_scores = setup_afinn(rank, comm)
    root = aho.aho_create_statemachine(word_scores.keys())
    grid_scores = grid.make_grid_dict()
    # Open the .json
    file, file_size = open_file(comm, file_name, rank)
    if not file:
        return grid_scores
    # Chunk of file to read
    chunk_size, chunk_offset, n_buffers = which_chunk(file_size, size, rank)

    logger.log("Commencing processing of chunk beginning at {} of {} of size {} at worker {}".format(
        chunk_offset, file_size, chunk_size, rank))
    for i in range(n_buffers):
        try:
            # Read the file into the buffer
            buffer = write_to_buffer(file, chunk_size, n_buffers, chunk_offset, i)
        except MemoryError:
            logger.log_error("Could not write file portion {}/{} to buffer at worker {}".format(i+1, n_buffers, rank))
        else:
            # Find all the tweets contained in the buffer and process them
            find_and_process_tweets(buffer, word_scores, grid_scores, root)
            logger.log("Succesfully parsed file portion {}/{} at worker {}".format(i+1, n_buffers, rank))
    file.Close()
    return grid_scores


def find_and_process_tweets(buffer, word_scores, grid_scores, root):
    tweets = re.findall(TWEET_REGEX, buffer, re.I)
    for tweet in tweets:
        try:
            cell, score = parse_single_tweet(tweet, word_scores, root, grid_scores)
            if cell:
                process_score(score, cell, grid_scores)
        except LookupError:
            logger.log_error("Could not process tweet")
            

def parse_single_tweet(tweet, word_scores, root, grid_dict):
    '''
    Extracts the contents and co-ordinates of the tweet and parses the result
    '''
    text = re.search(TEXT_REGEX, tweet).group(1)
    if not text:
        logger.log_error("Issue with searching tweet for text")
        return None, 0
    decoded = text.decode("utf-8")
    score = calculate_score(word_scores, decoded.lower(), root)
    coordinates = re.search(COORD_REGEX, tweet)
    if not coordinates:
        logger.log_error("Issue with searching tweet for co-ordinates")
        return None, 0
    long = float(coordinates.group(1))
    lat = float(coordinates.group(2))
    cell = grid.which_grid_cell(lat, long, grid_dict)
    return cell, score


def calculate_score(word_scores: dict, tweet: str, root) -> int:
    '''
    Calculates the sentiment score of a tweet. 
    Pattern matching using modified Aho-Corasick to only match on exact matches
    '''
    score = 0
    patterns_found = aho.aho_find_all(tweet, root)
    for pattern in patterns_found:
        score += word_scores[pattern]
    return score


def process_score(score, grid_cell, grid_scores):
    '''
    Increments the count of tweets encounter in a given cell and also 
    modifies the sentiment score of the cell accordingly
    '''
    if grid_cell == "A5" or grid_cell == "B5" or grid_cell == "D1" or grid_cell == "D2":
        logger.log("Error - we have an out of bounds cell being counted - {}".format(grid_cell))
    grid_scores[grid_cell]['score'] += score
    grid_scores[grid_cell]['count'] += 1



def print_results(result):
    '''
    Sends results to stdout in a clean format
    '''
    for cell in result:
        logger.log("{}, Tweet count: {}, Sentiment score: {}".format(
            cell, result[cell]['count'], result[cell]['score']))


def main(argv):
    '''
    Entrypoint into the process
    '''
    fname = argv[0]
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    if rank == 0:
        start_time = time.time()
        logger.log("Starting execution at {}".format(time.asctime()))
        run_main(comm, fname)
        logger.log("Finished execution at {} - took {}".format(time.asctime(), time.time() - start_time))
    else:
        run_main(comm, fname)
