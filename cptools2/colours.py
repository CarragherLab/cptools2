PURPLE = '\033[95m'
BLUE = '\033[94m'
GREEN = '\033[92m'
YELLOW = '\033[93m'
RED = '\033[91m'
ENDC = '\033[0m'
BOLD = '\033[1m'
UNDERLINE = '\033[4m'

def _colour(col):
    return col + "{}" + ENDC

def green(s):
    return _colour(GREEN).format(s)

def blue(s):
    return _colour(BLUE).format(s)

def underline(s):
    return _colour(UNDERLINE).format(s)

def bold(s):
    return _colour(BOLD).format(s)

def yellow(s):
    return _colour(WARNING).format(s)

def red(s):
    return _colour(FAIL).format(s)

def purple(s):
    return _colour(HEADER).format(s)
