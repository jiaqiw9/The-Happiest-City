
class AhoNode:
    def __init__(self):
        self.goto = {}
        self.out = []
        self.fail = None

def aho_create_forest(patterns):
    root = AhoNode()

    for path in patterns:
        node = root
        for symbol in path:
            node = node.goto.setdefault(symbol, AhoNode())
        node.out.append(path)
    return root


def aho_create_statemachine(patterns):
    root = aho_create_forest(patterns)
    queue = []
    for node in root.goto.values():
        queue.append(node)
        node.fail = root

    while len(queue) > 0:
        rnode = queue.pop(0)

        for key, unode in rnode.goto.items():
            queue.append(unode)
            fnode = rnode.fail
            while fnode != None and key not in fnode.goto.keys():
                fnode = fnode.fail
            unode.fail = fnode.goto[key] if fnode else root
            unode.out += unode.fail.out

    return root


def aho_find_all(s, root):
    node = root
    patterns = []
    for i in range(len(s)):
        while node != None and s[i] not in node.goto.keys():
            node = node.fail
        if node == None:
            node = root
            continue
        node = node.goto[s[i]]
        for pattern in node.out:
            # Only match complete matches
            if i + 1 == len(s) or s[i+1] == " " or s[i+1] == "," or s[i+1] == "!" or s[i+1] == "?" or s[i+1] == "." or s[i+1]=="'" or s[i+1] == "'":
                patterns.append(pattern)
                # print("Matched with " + pattern)
    return patterns