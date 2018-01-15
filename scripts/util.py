from os import makedirs, stat
from os.path import join, exists
from csv import DictReader
from hashlib import sha256
from ast import literal_eval
from difflib import SequenceMatcher
import errno
import glob


def levenshtein(s1, s2):
    if len(s1) < len(s2):
        return levenshtein(s2, s1)

    # len(s1) >= len(s2)
    if len(s2) == 0:
        return len(s1)

    previous_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = previous_row[
                             j + 1] + 1  # j+1 instead of j since previous_row and current_row are one character longer
            deletions = current_row[j] + 1  # than s2
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row

    return previous_row[-1]


def similarity(a, b):
    return SequenceMatcher(None, a, b).ratio()


def hash_factors(*args):
    result = ()
    for element in args:
        result += (element,)
    return sha256(str(result).encode()).hexdigest()


def remove_non_ascii(s):
    return "".join(i for i in s if ord(i) < 128)


def dict_diff(first, second):
    """Compare first dict with second dict.
       Returns a dict with keys found in second but not in first.
       And values are tuples of varibles from both"""
    diff = {}
    if not first:
        return second
    elif not second:
        return first
    if len(first) < len(second):
        first, second = second, first
    for key in second.keys():
        if key not in first.keys():
            diff[key] = second[key]
        elif first[key] != second[key]:
            diff[key] = (first[key], second[key])
    return diff


def convert_coordinates(string):
    """Converts coordinates from degrees, minutes and seconds to the equivalent float."""
    coordinate = string.replace(u'°', 'i-').replace("′", '-').replace('″', '-')
    multiplier = 1 if string[-1] in ['N', 'E'] else -1
    return multiplier * sum(float(x) / 60 ** n for n, x in enumerate(coordinate[:-2].split('-')))


def mk_dir(path):
    """creates dir if it doesn't exist.returns dir path"""
    if not path:
        return ""
    try:
        makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
    return path


def export_file(path, collection):
    if not path:
        return None
    try:
        with open(path, "w", encoding="utf-8") as file:
            if isinstance(list, dict):
                for url in collection:
                    file.write("%s\n" % url)
                    for line in collection[url]:
                        file.write("%s\n" % line)
                    file.write("------------------------------------------------------------------------------------\n")
                    file.write("\n")
                file.write("\n\n")
            else:
                for item in collection:
                    file.write("%s\n" % item)
                file.write("\n\n")
    except Exception as e:
        print(e)
    finally:
        return path


def import_file(path):
    if not path:
        raise Exception("No path specified to import!")
    rez = []
    for filename in glob.glob(join(path, '*.txt')):
        with open(filename, "r", encoding="utf-8") as f:
            try:
                for item in f.read().split("\n"):
                    if item:
                        rez.append(literal_eval(item))
            except SyntaxError as e:
                print(e)
    return rez


def import_csv(path):
    if not exists(path) or stat(path).st_size < 3:
        return None
    result = {}
    size = 0
    reader = DictReader(open(path, "r", encoding="utf-8-sig"))
    for row in reader:
        for column, value in row.items():
            result.setdefault(str(column), []).append(value)
        size += 1
    return result, size
