import itertools
import math
from difflib import SequenceMatcher
from math import sin, cos, radians


def ocrDataLocal(T, L, R, B, ocrData, matching_multiplier=(.25, .25, .5, .5)):
    """
        Parameters : Boundaries of Scope
        Output     : Returns that part of ocrdata which is confined within given boundaries
    """
    l_threshold = matching_multiplier[0]
    r_threshold = matching_multiplier[1]
    t_threshold = matching_multiplier[2]
    b_threshold = matching_multiplier[3]
    ocrDataLocal = []
    for data in ocrData:
        if (data['left'] + int(l_threshold * data['width']) >= L
                and data['right'] - int(r_threshold * data['width']) <= R
                and data['top'] + int(t_threshold * data['height']) >= T
                and data['bottom'] - int(b_threshold * data['height']) <= B):
            ocrDataLocal.append(data)
    return ocrDataLocal


def sort_ocr(data):
    data = sorted(data, key=lambda i: i['top'])
    for i in range(len(data) - 1):
        if abs(data[i]['top'] - data[i + 1]['top']) < 4:
            data[i + 1]['top'] = data[i]['top']
    data = sorted(data, key=lambda i: (i['top'], i['left']))
    return data


def needle_in_a_haystack(needle, haystack, proximity=None):
    words = needle.split()

    occurences = {}
    for hay in haystack:
        for word in words:
            if SequenceMatcher(None, hay['word'], word).ratio() >= 0.8:
                if word not in occurences:
                    occurences[word] = [hay]
                else:
                    occurences[word].append(hay)
    # pprint(occurences)
    all_combos = list(itertools.product(*occurences.values()))
    final_matches = []
    for combo in all_combos:
        min_left = min([word['left'] for word in combo])
        min_top = min([word['top'] for word in combo])
        max_right = max([word['right'] for word in combo])
        max_bottom = max([word['bottom'] for word in combo])
        combo_proxim = ocrDataLocal(min_top, min_left, max_right, max_bottom, haystack)
        combo_proxim = sort_ocr(combo_proxim)
        combo_proxim_str = ' '.join([word['word'] for word in combo_proxim])
        match_score = SequenceMatcher(None, combo_proxim_str.lower(), needle.lower()).ratio()
        # print('combo_proxim_str',combo_proxim_str,match_score)
        if match_score > 0.75:
            area = (max_bottom - min_top) * (max_right - min_left)
            midpoint = (min_left + int((max_right - min_left) / 2), min_top + int((max_bottom - min_top) / 2))
            final_matches.append(
                {'words': combo, 'top': min_top, 'y': min_top, 'x': min_left, 'left': min_left, 'right': max_right,
                 'bottom': max_bottom, 'score': match_score, 'area': area, 'midpoint': midpoint,
                 'height': max_bottom - min_top, 'width': max_right - min_left})

            # final_matches.append({'words':combo,'top':min_top,'left':min_left,'right':max_right,'bottom':max_bottom,'score':match_score,'area':area})
    # pprint(final_matches)
    if proximity:
        final_matches = sorted(final_matches, key=lambda x: (x['area'], -x['score']))[:3]
        return get_best_match(proximity, final_matches)

    final_matches = sorted(final_matches, key=lambda x: (x['area'], -x['score']))
    try:
        best_match = final_matches[0]
        if final_matches[0]['score'] > 0.6:
            return best_match
    except:
        return {}

    return {}


def get_rel_info(box1, box2, direction=None):
    mid1 = (
    box1['left'] + int(abs(box1['left'] - box1['right']) / 2), box1['top'] + int(abs(box1['top'] - box1['bottom']) / 2))
    mid2 = (
    box2['left'] + int(abs(box2['left'] - box2['right']) / 2), box2['top'] + int(abs(box2['top'] - box2['bottom']) / 2))

    dist = math.hypot(mid2[0] - mid1[0], mid2[1] - mid1[1])
    angle = math.atan((mid2[1] - mid1[1]) / (mid2[0] - mid1[0]))
    angle = abs(math.degrees(angle))
    angle = math.degrees(angle)

    if direction:
        if int(angle) in range(-30, 30):
            return 'left'
        else:
            return 'top'

    # print({'dist': dist, 'angle': angle})
    return {'dist': dist, 'angle': angle}


def point_pos(x0, y0, d, theta):
    theta_rad = radians(theta)
    return int(x0 + d * round(cos(theta_rad))), int(y0 + d * round(sin(theta_rad)))


def merge_fields(box_list, page_number=0):
    '''
    Merge 2 or more words and get combined coordinates
    '''
    if box_list and type(box_list[0]) is dict:
        # print('Merge',box_list)
        min_left = min([word['left'] for word in box_list])
        min_top = min([word['top'] for word in box_list])
        max_right = max([word['right'] for word in box_list])
        max_bottom = max([word['bottom'] for word in box_list])

        max_height = max_bottom - min_top
        total_width = max_right - min_left
        try:
            word = ' '.join([word['word'] for word in box_list])
        except:
            word = ''

        return {'height': max_height, 'width': total_width, 'y': min_top, 'x': min_left, 'right': max_right,
                'word': word.strip(), 'page': page_number}
    else:
        return {}


def get_best_match(inp, keysDict):
    # print('keysDict',keysDict)
    inpX = (inp['y'] + inp['y'] + inp['height']) / 2
    inpY = (inp['x'] + inp['x'] + inp['width']) / 2
    DistList = []
    for i, values in enumerate(keysDict):
        # Store all keywords,distances in a Dict
        # Get midpoint of the input
        midheight = ((keysDict[i]['top'] + keysDict[i]['bottom']) / 2)
        midwidth = ((keysDict[i]['left'] + keysDict[i]['right']) / 2)
        x = abs(midheight - inpX)
        y = abs(midwidth - inpY)
        dist = math.sqrt((x * x) + (y * y))
        DistList.append(round(dist, 2))
    # print("\nKey distance dictionary:\n%s" % DistList)
    closestKey = min(DistList)
    minIndex = DistList.index(closestKey)
    # print(keysDict[minIndex])
    return keysDict[minIndex]
