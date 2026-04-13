import os
import time
# TODO better error messages and handling

SAMPLE_NQ_FILE = "/home/adi/Desktop/keep/code/git/repos/the_grand_archive/maybe_useful/bm_eval/sample.nq"
FULL_NQ_FILE = "/home/adi/Desktop/keep/code/git/repos/the_grand_archive/maybe_useful/bm_eval/eval_full.nq"
TAIL_NQ_FILE = "/home/adi/Desktop/keep/code/git/repos/the_grand_archive/maybe_useful/bm_eval/out.nq"
DB_FILE = "bm_artifacts.db"

# Constants for fast byte-level comparisons
LESS_THEN = b'<'[0]  # 60
GREATER_THEN = b'>'[0]  # 62
DOT = b'.'[0]  # 46
DASH = b'-'[0]

SPACE = b' '[0]  # 32
NEWLINE = b'\n'[0]  # 10
CR =  b'\r'[0] # 13
TAB = b'\t'[0]

HASH = b'#'[0]  # 35
QUOTE = b'"'[0]  # 34
SINGLE_QUOTE = b'\''[0]
COLON = b':'[0]  # 58
UNDERSCORE = b'_'[0]  # 95
BACKSLASH = b'\\'[0]
HYPHEN = b'-'[0]       # 45

ESC_T = b't'[0]
ESC_B = b'b'[0]
ESC_N = b'n'[0]
ESC_R = b'r'[0]
ESC_F = b'f'[0]

ESC_CHARS = (ESC_T, ESC_B, ESC_N, ESC_R, ESC_F, QUOTE, SINGLE_QUOTE, BACKSLASH)

AT = b'@'[0]           # 64 (Language tags)
CARET = b'^'[0]        # 94 (Datatype markers)
U_LOWER = b'u'[0]      # 117 (Short Unicode escape \u)
U_UPPER = b'U'[0]      # 85 (Long Unicode escape \U)

A_LOWER = b'a'[0]
A_UPPER = b'A'[0]
Z_LOWER = b'z'[0]
Z_UPPER = b'Z'[0]
ZERO = b'0'[0]
NINE = b'9'[0]

HEX = [-1] * 256

for _c in b"0123456789abcdefABCDEF":
    HEX[_c] = int(chr(_c),16)

class NQParseError(Exception):
    pass

def multi_processing():
    """
    Coordinates multiprocessing across file chunks. 
    Note: Blank nodes (b-nodes) require coordination to ensure 
    the same label maps to the same ID across different processes.
    """
    pass


def divide_file_to_chunks(filename, chunk_size=1024 * 1024 * 64):
    """
        Calculates (start, end) byte offsets for the entire file.
        each chunk ends at a valid point i.e. a \n
    """
    file_size = os.path.getsize(filename)
    chunks = []
    start_ptr = 0

    with open(filename, 'rb') as f:

        while start_ptr < file_size:

            end_ptr = start_ptr + chunk_size

            if end_ptr > file_size:
                chunks.append((start_ptr, file_size))
                break

            f.seek(end_ptr) # jump to reading head to end pointer
            buffer_size = 4096
            found_new_line = False
            new_end_of_chunk = 0

            while not found_new_line:
                # loop until you find a new line or an EOF
                absolute_position_in_file = f.tell()
                buf = f.read(buffer_size) # note f.read does move the head pointer

                if not buf:
                    # reached the EOF
                    found_new_line = True
                    new_end_of_chunk =  file_size
                else:

                    new_line_index = buf.find(NEWLINE)
                    if new_line_index != -1:
                        # found a new line
                        new_end_of_chunk = absolute_position_in_file + new_line_index + 1
                        found_new_line = True

            chunks.append((start_ptr, new_end_of_chunk))
            start_ptr = new_end_of_chunk

    return chunks


def stream_quads(file_name, chunk_size=1024 * 1024 * 64, fast = False):
    """
    Coordinates the reading of chunks and yields individual quads.
    This is the 'Source' for your output stream.
    """
    offsets = divide_file_to_chunks(file_name, chunk_size)

    with open(file_name, 'rb') as f:
        for start, end in offsets:
            # Move the read head to the start of our verified chunk
            f.seek(start)
            # Read the bytes between start and end
            chunk_bytes = f.read(end - start)

            # Use 'yield from' to pass quads up from the chunk parser
            if fast:
                yield from parse_chunk_fast(chunk_bytes)
            else:
                yield from parse_chunk(chunk_bytes)


def parse_chunk_fast(chunk):
    i = 0
    length = len(chunk)

    while i < length:

        try:

            while i < length:
                if chunk[i] not in (CR, SPACE, NEWLINE, TAB):
                    break
                i += 1

            if i >= length:
                break
            # skip comments
            if chunk[i] == HASH:
                new_line_index = chunk.find(NEWLINE, i)
                i = new_line_index + 1 if new_line_index != -1 else length
                continue

            # skip_intra_line_empty_spaces
            while chunk[i] in (SPACE, TAB):
                i += 1

            # parse_subject
            if chunk[i] == LESS_THEN:
                # IRIREF
                next_greater_then_index = chunk.find(GREATER_THEN, i)
                if next_greater_then_index != -1:
                    subject = chunk[i + 1:next_greater_then_index]
                    i = next_greater_then_index + 1
                else:
                    raise NQParseError("malformed quad IRI must close with a > char")

            elif chunk[i] == UNDERSCORE:

                if chunk[i + 1] == COLON:
                    end = i + 2
                    while chunk[end] not in (SPACE, TAB):
                        if chunk[end] == NEWLINE:
                            raise NQParseError("Blank node can't be separated by a new line \\n")
                        end += 1

                    if end == i + 2:
                        raise NQParseError("Empty Blank Node label")

                    subject = chunk[i:end]
                    i = end

                else:
                    raise NQParseError("malformed b_node")

            else:
                raise NQParseError(f"subject can only be IRI or be node found {chunk[i]} instead")

            # predicate

            # skip_intra_line_empty_spaces
            start = i
            while chunk[start] in (SPACE, TAB):
                start += 1

            if start == i:
                raise NQParseError("no spaces between subject and predicate")

            i = start

            if chunk[i] == LESS_THEN:
                next_greater_then_index = chunk.find(GREATER_THEN, i)
                if next_greater_then_index != -1:
                    predicate = chunk[i + 1:next_greater_then_index]
                    i = next_greater_then_index + 1
                else:
                    raise NQParseError("malformed quad IRI must close with a > char")

            else:
                raise NQParseError("predicate can only be a valid IRIREf")

            # object
            # obj, i = parse_object(chunk, i, length)
            start = i
            while chunk[start] in (SPACE, TAB):
                start += 1

            if start == i:
                raise NQParseError("no spaces between predicate and object")

            i = start
            c = chunk[i]

            if c == LESS_THEN:
                next_greater_then_index = chunk.find(GREATER_THEN, i)
                if next_greater_then_index != -1:
                    val = chunk[i + 1:next_greater_then_index]
                    i = next_greater_then_index + 1
                else:
                    raise NQParseError("malformed quad IRI must close with a > char")
                #
                # ( type, literal /  IRIREF / b_node, language / IRIREF (Dtype), region, direction)  i
                obj = ("IRIREF", val, None, None, None)

            elif c == UNDERSCORE:
                if chunk[i + 1] == COLON:
                    end = i + 2
                    while chunk[end] not in (SPACE, TAB):
                        if chunk[end] == NEWLINE:
                            raise NQParseError("Blank node can't be separated by a new line \\n")
                        end += 1

                    if end == i + 2:
                        raise NQParseError("Empty Blank Node label")

                    val = chunk[i:end]
                    i = end
                else:
                    raise NQParseError("malformed b_node")

                obj = ("b_node", val, None, None, None)

            ###################################
            elif c == QUOTE:
                #################################

                literal, i = extract_literal_raw(chunk, i, length)
                language, region, direction, IRIREF = None, None, None, None

                c = chunk[i]
                if c == AT:
                    i += 1
                    start = i
                    has_dash = False
                    while i < length:
                        c = chunk[i]
                        if c == SPACE or c == TAB:
                            break
                        elif (A_LOWER <= c <= Z_LOWER) or A_UPPER <= c <= Z_UPPER:
                            i += 1
                        elif c == DASH:
                            i += 1
                            has_dash = True
                            break
                        else:
                            raise NQParseError("malformed quad literal ")

                    if has_dash:
                        language = chunk[start:i]
                        region_idx = i
                        has_direction = False
                        while chunk[i] not in (SPACE, TAB):
                            c = chunk[i]
                            if A_LOWER <= c <= Z_LOWER or A_UPPER <= c <= Z_UPPER or ZERO <= c <= NINE:
                                i += 1
                            elif c == DASH:
                                i += 1
                                if chunk[i] == DASH:
                                    has_direction = True
                                    region = chunk[region_idx: i - 1]
                                    i += 1
                                    break
                            else:
                                raise NQParseError("malformed quad literal ")

                        if region_idx == i:
                            raise NQParseError("malformed quad literal found space after - in language tag")
                        if has_direction:
                            direction_start = i
                            while chunk[i] not in (SPACE, TAB):
                                c = chunk[i]
                                if A_LOWER <= c <= Z_LOWER or A_UPPER <= c <= Z_UPPER:
                                    i += 1
                                else:
                                    raise NQParseError("malformed literal direction ")

                            if direction_start == i:
                                raise NQParseError("malformed language tag found space after -- in direction ")
                            direction = chunk[direction_start: i]

                    else:
                        language = chunk[start:i]

                elif c == CARET:
                    if chunk[i + 1] == CARET and chunk[i + 2] == LESS_THEN:

                        next_greater_then_index = chunk.find(GREATER_THEN, i)
                        if next_greater_then_index != -1:
                            IRIREF = chunk[i + 3:next_greater_then_index]
                            i = next_greater_then_index + 1
                        else:
                            raise NQParseError("malformed quad IRI must close with a > char")

                    else:
                        raise NQParseError("malformed IRIREF ")

                elif c == SPACE or c == TAB:
                    pass
                else:
                    raise NQParseError(f"malformed quad literal char {c} found after end of literal only a space"
                                       f", @, ^^ are allowed after literal")

                #################################
                if IRIREF:
                    obj = ("literal_data_tag", literal, IRIREF, region, direction)
                else:
                    obj = ("literal_lang_tag", literal, language, region, direction)

            ###################################

            else:
                raise NQParseError("malformed quad object can only be an IRIREF , a b_node, or a literal ")

            # graph and dot
            start = i
            while chunk[start] in (SPACE, TAB):
                start += 1

            if start == i:
                raise NQParseError("no spaces between object possible graph")

            i = start
            c = chunk[i]

            if c == LESS_THEN:
                graph, i = parse_IRIREF(chunk, i, length)
            elif c == UNDERSCORE:
                graph, i = parse_blank_node(chunk, i, length)
            elif c == DOT:
                # return None, i
                pass
            else:
                raise NQParseError(f"malformed quad optional graph allowed to be IRIREF"
                                   f" or b_node found {chunk[i - 1: i]} instead")

            while i < length and chunk[i] in {SPACE, TAB}:
                i += 1
            if chunk[i] != DOT:
                raise NQParseError(f" malformed line expected a DOT found {chunk[i: i + 1]}")

            i += 1

            # go to end of line

            while i < length and chunk[i] in {SPACE, TAB, CR}:
                i += 1

            if i >= length:
                yield subject, predicate, obj, graph

            elif chunk[i] == HASH:
                newline_idx = chunk.find(NEWLINE, i)
                i = newline_idx + 1 if newline_idx != -1 else length

            elif chunk[i] == NEWLINE:
                i += 1
            else:
                raise NQParseError(" after a . there can only be a comment or a newline no other chars are allowed")

            #

            yield subject, predicate, obj, graph

        except Exception  as e:
            print(e)
            # skip bad line
            new_line_index = chunk.find(NEWLINE, i)
            i = new_line_index + 1 if new_line_index != -1 else length
            continue



def parse_chunk(chunk):
    """
    Iterates through a chunk and yields valid quads.
    Expects chunk boundaries aligned to quad boundaries.
    """
    i = 0
    length = len(chunk)

    while i < length:

        try:
            i = skip_empty_spaces(chunk, i, length )
            if i >= length:
                break
            # skip comments
            if chunk[i] == HASH:
                new_line_index = chunk.find(NEWLINE, i)
                i = new_line_index + 1 if new_line_index != -1 else length
                continue
            i = skip_intra_line_empty_spaces(chunk, i, length)
            subject, i = parse_subject(chunk, i, length)
            predicate, i = parse_predicate(chunk, i, length)
            obj, i = parse_object(chunk, i, length)
            graph, i = parse_possible_graph_and_dot(chunk, i, length)
            i = go_to_end_of_line(chunk, i, length)

            yield subject, predicate, obj, graph

        except Exception  as e:
            print(e)
            # skip bad line
            new_line_index = chunk.find(NEWLINE, i)
            i = new_line_index + 1 if new_line_index != -1 else length
            continue

def skip_intra_line_empty_spaces(chunk,i ,length):

    while i < length  and chunk[i] in (SPACE, TAB):
        i += 1

    return i

def skip_empty_spaces(chunk, i, length ):
    """this method is used to skip empty lines"""
    while i < length:
        if chunk[i] not in (CR, SPACE, NEWLINE, TAB):
            break
        i += 1

    return i

def parse_subject(chunk, i, length):

    if chunk[i] == LESS_THEN:
        return parse_IRIREF(chunk , i, length)

    elif chunk[i] == UNDERSCORE:
        return parse_blank_node(chunk , i, length)
    else:
        raise NQParseError(f"subject can only be IRI or be node found {chunk[i] } instead")

def parse_predicate(chunk, i, length):
    start = skip_intra_line_empty_spaces(chunk, i, length)
    if start == i :
        raise NQParseError("no spaces between subject and predicate")
    i = start
    if chunk[i] == LESS_THEN:
        return parse_IRIREF(chunk, i, length)
    else:
        raise NQParseError("predicate can only be a valid IRIREf")

def parse_object(chunk, i, length):
    """
    returns a tuple ( type, literal /  IRIREF / b_node, language / IRIREF (Dtype), region, direction)  i
    """
    start = skip_intra_line_empty_spaces(chunk, i, length)
    if start == i:
        raise NQParseError("no spaces between predicate and object")
    i = start
    c = chunk[i]
    if c == LESS_THEN:
        val, i = parse_IRIREF(chunk, i, length)
        # ( type, literal /  IRIREF / b_node, language / IRIREF (Dtype), region, direction)  i
        return ("IRIREF", val, None, None, None), i
    elif c == UNDERSCORE:
        val , i = parse_blank_node(chunk, i, length)
        return ("b_node", val, None, None, None), i
    elif c == QUOTE:
        literal, language, region, direction, IRIREF, i =  parse_literal(chunk, i, length)
        if IRIREF:
            return ("literal_data_tag", literal, IRIREF, region, direction), i
        return ("literal_lang_tag", literal, language, region, direction), i
    else:
        raise NQParseError("malformed quad object can only be an IRIREF , a b_node, or a literal ")

def parse_possible_graph_and_dot(chunk, i, length):

    start = skip_intra_line_empty_spaces(chunk, i , length)
    if start == i:
        raise NQParseError("no spaces between object possible graph")

    i = start
    c = chunk[i]

    if  c == LESS_THEN:
        graph, i =  parse_IRIREF(chunk, i, length)
    elif c == UNDERSCORE:
        graph, i = parse_blank_node(chunk , i, length)
    elif c == DOT:
        return None , i
    else:
        raise NQParseError(f"malformed quad optional graph allowed to be IRIREF"
                           f" or b_node found {chunk[i- 1: i] } instead")


    while i < length and chunk[i] in {SPACE, TAB}:
        i += 1
    if  chunk[i] != DOT:
        raise NQParseError(f" malformed line expected a DOT found {chunk[i: i+1]}")

    return graph, i + 1

def parse_IRIREF(chunk, i, length):
    next_greater_then_index = chunk.find(GREATER_THEN, i)
    if next_greater_then_index != -1:
        iriref = chunk[i + 1:next_greater_then_index]
        i = next_greater_then_index + 1
        return iriref, i
    else:
        raise NQParseError("malformed quad IRI must close with a > char")

def parse_blank_node(chunk, i, length):

    if chunk[i + 1] == COLON:
        end = i + 2
        while chunk[end] not in ( SPACE, TAB):
            if chunk[end] == NEWLINE:
                raise NQParseError("Blank node can't be separated by a new line \\n")
            end += 1

        if end == i + 2:
            raise NQParseError("Empty Blank Node label")

        subject = chunk[i:end]
        i = end
        return subject, i
    else:
        raise NQParseError("malformed b_node")

def parse_literal(chunk, i, length):

    literal, i = extract_literal_raw(chunk, i, length)
    language , region, direction, IRIREF = None, None, None, None

    c = chunk[i]
    if c == AT:
        i += 1
        start = i
        has_dash = False
        while i < length:
            c = chunk[i]
            if c == SPACE or c == TAB:
                break
            elif ( A_LOWER <= c <= Z_LOWER  ) or A_UPPER <= c <= Z_UPPER  :
                i+=1
            elif c == DASH:
                i+=1
                has_dash = True
                break
            else:
                raise NQParseError("malformed quad literal ")

        if has_dash:
            language = chunk[start:i]
            region_idx = i
            has_direction = False
            while chunk[i] not in ( SPACE, TAB ):
                c = chunk[i]
                if A_LOWER <= c <= Z_LOWER or A_UPPER <= c <= Z_UPPER or ZERO <= c <= NINE:
                    i += 1
                elif c == DASH:
                    i+=1
                    if chunk[i] == DASH:
                        has_direction = True
                        region = chunk[region_idx: i -1]
                        i+=1
                        break
                else:
                    raise NQParseError("malformed quad literal ")

            if region_idx == i:
                raise NQParseError("malformed quad literal found space after - in language tag")
            if has_direction:
                direction_start = i
                while chunk[i] not in ( SPACE, TAB ):
                    c = chunk[i]
                    if A_LOWER <= c <= Z_LOWER or A_UPPER <= c <= Z_UPPER:
                        i+=1
                    else: raise NQParseError("malformed literal direction ")

                if direction_start == i:
                    raise NQParseError("malformed language tag found space after -- in direction ")
                direction = chunk[direction_start: i]

        else:
            language = chunk[start:i]
        return literal, language, region, direction, IRIREF, i

    elif c == CARET:
        if chunk[i+1] == CARET and chunk[i+2] == LESS_THEN:
            IRIREF, new_i  = parse_IRIREF(chunk, i+2, length)
            return literal, language, region, direction, IRIREF, new_i
        else:
            raise NQParseError("malformed IRIREF ")

    elif c == SPACE or c == TAB :
        return literal, language, region, direction, IRIREF , i
    else:
        raise NQParseError(f"malformed quad literal char {c} found after end of literal only a space"
                           f", @, ^^ are allowed after literal")


def extract_literal_raw(chunk, i, length):
    # started from a " so we go over 1
    i += 1
    start = i
    while i < length:
        c = chunk[i]
        if c == BACKSLASH:
            i+=1
            if c in ESC_CHARS:
                i += 1
                continue

            elif c == U_LOWER:
                if i + 4 >= length:
                    raise NQParseError("invalid hex in \\u escape")
                code = 0
                for j in range(1, 5):
                    c_hex = HEX[chunk[i + j]]
                    if c_hex == -1:
                        raise NQParseError("invalid hex in \\u escape")
                    code = (code << 4) | c_hex
                if 0xD800 <= code <= 0xDFFF:
                    raise NQParseError("invalid unicode code point")

                i += 5
                continue

            elif c == U_UPPER:
                if i + 8 >= length:
                    raise NQParseError("invalid hex in \\U escape")

                code = 0
                for j in range(1, 9):
                    c_hex = HEX[chunk[i + j]]
                    if c_hex == -1:
                        raise NQParseError("invalid hex in \\U escape")
                    code = (code << 4) | c_hex

                if code > 0x10FFFF or (0xD800 <= code <= 0xDFFF):
                    raise NQParseError("invalid unicode code point")

                i += 9
                continue

            else :
                raise NQParseError("malformed quad literal")


        if c == QUOTE:
            return chunk[start: i] , i + 1
        # Strict Rule: Literal may not contain LF or CR unescaped
        elif c == NEWLINE or c == CR:
            raise NQParseError("Unescaped newline in literal")

        i += 1

    raise NQParseError("malformed quad literal unexpected end of file")

def go_to_end_of_line(chunk, i, length):
    while i < length and chunk[i] in {SPACE, TAB, CR}:
        i += 1
    if i >= length:
        return i

    elif chunk[i] == HASH:
        newline_idx = chunk.find(NEWLINE, i)
        return newline_idx + 1 if newline_idx != -1 else length

    elif chunk[i] == NEWLINE:
        return i + 1
    else:
        raise NQParseError(" after a . there can only be a comment or a newline no other chars are allowed")

def test(filename):
    # Usage (The "Consumer" side)
    chunk_size = 1024*1024*64
    for quad in stream_quads(filename, chunk_size, fast=True):
        pass

start_time = time.perf_counter()
test(SAMPLE_NQ_FILE)
end_time = time.perf_counter()
duration = end_time - start_time
print(f"Function took {duration:.6f} seconds")