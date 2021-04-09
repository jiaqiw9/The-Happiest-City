import json
import re
from mpi4py import MPI


def process_line(line):
    remove_at_end = [2, 3]
    tweet = None
    for r in remove_at_end:
        try:
            tweet = json.loads(line[:-r])
        except:
            pass
        else:
            break
    if tweet is not None:
        area = ""
        total_score = 0

        coordinate = tweet["value"]["geometry"]["coordinates"]
        for feature in melbGrid_json["features"]:
            if feature["properties"]["xmin"] < coordinate[0] <= feature["properties"]["xmax"] and feature["properties"]["ymin"] < coordinate[1] <= feature["properties"]["ymax"]:
                area = feature["properties"]["id"]

        if area in cell_score:    
            for word, score in score_dict.items():
                pattern = r'[ \'"]' + word + r'[ \!,\?\.\'"]'
                total_score += score * len(re.findall(pattern, tweet["value"]["properties"]["text"].lower()))
            cell_score[area]["tweets"] += 1
            cell_score[area]["score"] += total_score

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

# read AFINN dictionary
score_dict = dict()
with open('AFINN.txt') as AFINN:
    for line in AFINN:
        line_split = line.rsplit(maxsplit = 1)
        score_dict[line_split[0]] = int(line_split[1])

# Grid dictionary
cell_score = dict()
with open('melbGrid.json') as melbGrid:
    melbGrid_json = json.load(melbGrid)
    for feature in melbGrid_json["features"]:
        cell_score[feature["properties"]["id"]] = {"tweets":0, "score":0}

# read and process json
data_set = 'tinyTwitter.json'
line_count = 0
with open(data_set) as Twitter:
    for line in Twitter:
        if line_count % size == rank:
            process_line(line)
        line_count += 1
cell_score = comm.gather(cell_score, root = 0)
if rank == 0:
    for cell_score_ in cell_score[1:]:
        for area, total_score in cell_score_.items():
            cell_score[0][area]["tweets"] += cell_score_[area]["tweets"]
            cell_score[0][area]["score"] += cell_score_[area]["score"]
    print(cell_score[0])
