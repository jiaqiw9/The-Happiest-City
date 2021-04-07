import math

# Constants for assigning a coordinate to a region on the GRID
X_MIN, X_MAX = 144.7, 145.45 
Y_MIN, Y_MAX = -37.5, -38.1
Y_ORDS = ['A','B','C','D']
X_ORDS = ['1','2','3','4','5']
INTERVAL = 0.15


# Test1: (144.75, -37.6) -> A1
test1 =  (144.75, -37.6)
# Test2: (144.9, -37.9) -> C2
test2 = (144.9, -37.9)
# Test3: (145.34, -37.85) -> C5
test3 = (145.34, -37.85)
# Test4: (145.5, -37.6) -> None
test4 = (145.5, -37.6)


def get_grid(x, y):
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
            print(name)
            return name

get_grid(test1[0], test1[1])
get_grid(test2[0], test2[1])
get_grid(test3[0], test3[1])
get_grid(test4[0], test4[1])