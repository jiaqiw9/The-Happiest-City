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
        if coordinate[1] <= -37.500000 and coordinate[1] > -37.650000:
            area += "A"
        elif coordinate[1] <= -37.650000 and coordinate[1] > -37.800000:
            area += "B"
        elif coordinate[1] <= -37.800000 and coordinate[1] > -37.950000:
            area += "C"
        elif coordinate[1] <= -37.950000 and coordinate[1] > -38.100000:
            area += "D"
        if coordinate[0] > 144.700000 and coordinate[0] <= 144.850000:
            area += "1"
        elif coordinate[0] > 144.850000 and coordinate[0] <= 145.000000:
            area += "2"
        elif coordinate[0] > 145.000000 and coordinate[0] <= 145.150000:
            area += "3"
        elif coordinate[0] > 145.150000 and coordinate[0] <= 145.300000:
            area += "4"
        elif coordinate[0] > 145.300000 and coordinate[0] <= 145.450000:
            area += "5"

        if area in cell_score:    
            for word, score in score_dict.items():
                pattern = r' ' + word + r'[\!,\?\.\'"]'
                total_score += score * len(re.findall(pattern, tweet["value"]["properties"]["text"]))
            cell_score[area]["tweets"] += 1
            cell_score[area]["score"] += total_score

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

# read json
data_set = 'tinyTwitter.json'
with open(data_set) as Twitter:
    for line in Twitter:
        process_line(line)
print(cell_score)

