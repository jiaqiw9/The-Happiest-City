# read AFINN dictionary
score_dict = dict()
with open('AFINN.txt') as AFINN:
    for line in AFINN:
        line_split = line.rsplit(maxsplit = 1)
        score_dict[line_split[0]] = int(line_split[1])
