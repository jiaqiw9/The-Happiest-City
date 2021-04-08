import json

def make_grid_dict():
    # Grid dictionary
    cell_score = dict()
    with open('melbGrid.json', 'r') as melbGrid:
        melbGrid_json = json.load(melbGrid)
        for feature in melbGrid_json["features"]:
            props = feature["properties"]
            id = props.pop('id')
            props['count'] = 0
            props['score'] = 0
            cell_score[id] = props
    return cell_score

def which_grid_cell(x, y, grid_dict):
    cell = None
    for c in grid_dict:
        if grid_dict[c]['xmin'] <= x <= grid_dict[c]['xmax'] and grid_dict[c]['ymin'] <= y <= grid_dict[c]['ymax']:
            cell = c
            break
    return cell