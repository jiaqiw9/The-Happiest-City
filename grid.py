import json, math
# Constants for assigning a coordinate to a region on the GRID
X_MIN, X_MAX = 144.7, 145.45
Y_MIN, Y_MAX = -37.5, -38.1
Y_ORDS = ['A', 'B', 'C', 'D']
X_ORDS = ['1', '2', '3', '4', '5']
INTERVAL = 0.15


def make_grid_dict():
    # Grid dictionary
    cell_score = dict()
    with open('melbGrid.json', 'r') as melbGrid:
        melbGrid_json = json.load(melbGrid)
        for feature in melbGrid_json["features"]:
            props = feature["properties"]
            cell_id = props.pop('id')
            props['count'] = 0
            props['score'] = 0
            cell_score[cell_id] = props
    return cell_score

def which_grid_cell(x, y, grid_dict):
    cell = None
    for c in grid_dict:
        if grid_dict[c]['xmin'] <= x <= grid_dict[c]['xmax'] and grid_dict[c]['ymin'] <= y <= grid_dict[c]['ymax']:
            cell = c
            break
    return cell

def get_grid_cell(x, y):
    '''
    Determines which grid cell a given point (represented by a latitude and longitude) belongs to.
    '''
    if y > Y_MIN or y < Y_MAX or x < X_MIN or x > X_MAX or (y > -37.8 and x > 145.3) or (y < -37.95 and x < 145):
        return None  # Out of bounds - not in Melbourne
    else:
        y_index = math.floor(abs(y - Y_MIN) / INTERVAL)
        x_index = math.floor(abs(x - X_MIN) / INTERVAL)
        if x_index > 4 or y_index > 3:
            # logger.log_error(
            #     "Error occurred when finding the grid cell associated with the point ({},{})".format(x, y))
            return None
        else:
            name = Y_ORDS[y_index] + X_ORDS[x_index]
            return name