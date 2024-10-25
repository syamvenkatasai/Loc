

#--------------------------------------------------------------------------------------------------

      
def char_check(dict1,dict2):
    if dict2:
        word2=dict2['word']
    else:
        word2=''
    if dict1:
        word1=dict1['word']
    else:
        word1=''
    if ':' in word1:
        colon_index = word1.index(':')
        if colon_index==len(word1)-1:
            return False
    if ':' in word2:
        colon_index = word2.index(':')
        if colon_index==0:
            return False
    return True       


#--------------------------------------------------------------------------------------------------


def lines_horizontal(words_):
    for words in words_:
        # Sort the words by their 'top' position (vertical position)
        sorted_words = sorted(words, key=lambda x: x["top"])

        # Group words on the same horizontal line
        line_groups = []
        current_line = []
        for word in sorted_words:
#             #print(word)
            if not current_line:
                current_line.append(word)
            else:
                diff=abs(word["top"] - current_line[-1]["top"])
                if diff < 2:
                    # Word is on the same line as the previous word
                    current_line.append(word)
                else:
                    # Word is on a new line
                    line_groups.append(current_line)
                    current_line = [word]

            # Add the last line to the groups
        if current_line:
            line_groups.append(current_line)

    return line_groups


#--------------------------------------------------------------------------------------------------


def find_below_elements(centorid,centriod_left,centroid_right,sorted_word_list,first_word,next_word,word_top,horizontal_x):
    if first_word not in sorted_word_list:
        #print(f"{first_word} not in sorted list")
        return [],2
    return_list=[]
#     #print(first_word,next_word)
    if next_word:
        spacing=next_word['left']-first_word['right']
    else:
        spacing=0
    check=char_check(first_word,next_word)
    #print(f"first_word is {first_word} next_word is {next_word} and x spacing is {spacing} and check is {check} \n")
    if (abs(spacing) > horizontal_x or horizontal_x ==0 or next_word=={}) and check:
        for word in sorted_word_list:
            if word['left']<=centorid<=word['right'] and word['top'] >= word_top:
                if word not in return_list:
                    return_list.append(word)
                    api=1
            if word['left']<=centriod_left<=word['right'] and word['top'] >= word_top:
                if word not in return_list:
                    return_list.append(word)
                    api=1
            if word['left']<=centroid_right<=word['right'] and word['top'] >= word_top:
                if word not in return_list:
                    return_list.append(word)
                    api=1
    else:
        return_list.append(first_word)
        api=0
    return return_list,api


#--------------------------------------------------------------------------------------------------



def find_x_of_y(word,hori_lines):
    found_dicts = []
    check=True
    for sublist in hori_lines:
        for dictionary in sublist:
            if dictionary == word:
                found_dicts = sublist
                found_dicts.sort(key=lambda x: x["left"])  # Sort the list of dictionaries by the "left" key
                break
    for i in range(len(found_dicts)):
        dictionary=found_dicts[i]
        if dictionary == word and i != len(found_dicts)-1:
            next_word=found_dicts[i+1]
            check=char_check({},next_word)
            break
    return check
 
    
#--------------------------------------------------------------------------------------------------


def find_right_below(word_list,hori_lines):
    if not word_list:
        return []
    return_list=[word_list[0]]
    previous_word=word_list[0]
    for word in word_list[1:]:
        Y_spacing = word['top'] - previous_word['bottom']
        vertical_spacing = word['top'] - previous_word['bottom']
        #print(f"vertical spacing btw {word} and {previous_word} is {vertical_spacing}\n")
        if char_check(word,{}):
            check_y_x=find_x_of_y(word,hori_lines)
        else:
            check_y_x=False
        if vertical_spacing <= (Y_spacing) and Y_spacing <= 7 and check_y_x:
            return_list.append(word)
            #print(f"vertical spacing is {vertical_spacing} and {return_list} \n")
            previous_word = word
        else:
            #print(f"vertical spacing is {vertical_spacing} breaking \n")
            break
    return return_list



#--------------------------------------------------------------------------------------------------



def find_vertical_y(elements):
    if len(elements)== 2:
        first_element = elements[0]
        second_element = elements[1]
        spacing1 = second_element['top'] - first_element['bottom']
        return 10
    elif len(elements) <= 1:
        return 0
    first_element = elements[0]
    second_element = elements[1]
    third_element = elements[2]
    #print(first_element,second_element,third_element)
    spacing1 = second_element['top'] - first_element['bottom']
    # Calculate the vertical spacing between the next two elements
    spacing2 = third_element['top'] - second_element['bottom']
    #print(spacing2,spacing1)
    if abs(spacing2-spacing1) < 40:
        return max(spacing2,spacing1)
    else:
        return 0

    
#--------------------------------------------------------------------------------------------------
   
    
    
def combine_dicts(dicts):
    combined_dict = {
        "word": ' '.join([d["word"] for d in dicts]),
        "width": max([d["width"] for d in dicts]),
        "height": max([d["height"] for d in dicts]),
        "top": min([d["top"] for d in dicts]),
        "left": min([d["left"] for d in dicts]),
        "bottom": max([d["bottom"] for d in dicts]),
        "right": max([d["right"] for d in dicts]),
        "confidence": max([d["confidence"] for d in dicts]),
        "sen_no": dicts[0]["sen_no"],  # Assuming they all have the same sen_no
        "pg_no": dicts[0]["pg_no"],# Assuming they all have the same pg_no
#         "x-space":dicts[0]["x-space"]
    }
    return combined_dict


#--------------------------------------------------------------------------------------------------


def removing_found_items(sorted_word_list,right_below_words):
    set1 = {frozenset(d.items()) for d in sorted_word_list}
    set2 = {frozenset(d.items()) for d in right_below_words}
    unique_dicts_in_list1 = [dict(fs) for fs in (set1 - set2)]
    return unique_dicts_in_list1



#--------------------------------------------------------------------------------------------------
# we have a thrushold for this is horizontal_x

def get_ocr_block(data):
    #xspacing should come from the ocr_word formation data
    horizontal_x=100
    sorted_word_list = sorted(data[0], key=lambda word: word["top"])
    # #print(sorted_word_list)
    i=0
    blocks=[sorted_word_list]
    temp=[]
    word=sorted_word_list[i]
    hori_lines=lines_horizontal(data)
    #print(hori_lines)
    required_list=[]
    for hori_line in hori_lines:
        hori_line = sorted(hori_line, key=lambda x: x['left'])
        if len(sorted_word_list)==0:
            break
        #print(f"hori_line is {hori_line} \n")
        if len(hori_line)==1:
            first_word=hori_line[0]
            word_top=first_word['top']
            centriod_left=first_word['left']
            centroid_right=first_word['right']
            centorid=(first_word['left']+first_word['right'])/2
            #print(f" centorid is {centorid} \n")

            below_words,x_test=find_below_elements(centorid,centriod_left,centroid_right,sorted_word_list,first_word,{},word_top,0)
            #print(f" below_words is {below_words} \n")
            if not x_test:
                right_below_words=below_words
            else:
                below_words = sorted(below_words, key=lambda word: word["top"])
                right_below_words=find_right_below(below_words,hori_lines)
    #         vertical_y=find_vertical_y(below_words)


            #print(f" right_below_words is {right_below_words} \n")

            if right_below_words: 
                sorted_word_list=removing_found_items(sorted_word_list,right_below_words)
                #print(f" removing_found_items is {right_below_words} \n")
                required_list.append(right_below_words)
            else:
                if first_word in sorted_word_list:
                    required_list.append(hori_line) 
                sorted_word_list=removing_found_items(sorted_word_list,hori_line)
                #print(f" removing_found_items is {hori_line} \n")
            sorted_word_list = sorted(sorted_word_list, key=lambda word: word["top"])
    #         #print(f" sorted is {sorted_word_list}")
            #print(f"{required_list} \n")
        else:
            for i,lines in enumerate(hori_line):
                first_word=lines
                if i<len(hori_line)-1:
                    second_word=hori_line[i+1]
                else:
                    second_word={}
                word_top=first_word['top']
                
                centorid=(first_word['left']+first_word['right'])/2
                centriod_left=first_word['left']
                centroid_right=first_word['right']
                #print(f" centorid is {centorid} \n")

                below_words,x_test=find_below_elements(centorid,centriod_left,centroid_right,sorted_word_list,first_word,second_word,word_top,horizontal_x)
                #print(f" below_words is {below_words} \n")
                below_words = sorted(below_words, key=lambda word: word["top"])
                if not x_test:
                    right_below_words=below_words
                else:
                    below_words = sorted(below_words, key=lambda word: word["top"])
                    right_below_words=find_right_below(below_words,hori_lines)
    #             vertical_y=find_vertical_y(below_words)
    #             #print(f" vertical_y is {vertical_y}")


                #print(f" right_below_words is {right_below_words} \n")


                if right_below_words:
                    sorted_word_list=removing_found_items(sorted_word_list,right_below_words)
                    #print(f" removing_found_items is {right_below_words} \n")
                    required_list.append(right_below_words)
                else:
                    sorted_word_list=removing_found_items(sorted_word_list,[lines])
                    #print(f" removing_found_items is {lines} \n")
                    if first_word in sorted_word_list:
                        required_list.append([lines]) 

                sorted_word_list = sorted(sorted_word_list, key=lambda word: word["top"])
    #             #print(f" sorted is {sorted_word_list}")
                #print(F"{required_list}  \n")
    #         #print("\n")
    # #print(f"final is{required_list}")
    data_ocr=[]
    for lists in required_list:
    #     #print(F"lists is {lists}")
        combined_result = combine_dicts(lists)
        data_ocr.append(combined_result)
    #     #print(F"main is {data_ocr}")
    
#     #print(f" \n final main is {data_ocr}")
    data_ocr=combine_boxes(data_ocr)
    #print(f" \n final main is {data_ocr}")
    return data_ocr




#--------------------------------------------------------------------------------------------------


def combine_boxes(data):
#     #print(data)
    for main_box in data:
        #print(f"main box is {main_box}")
        boxes_inside_container=[]
        check=char_check(main_box,main_box)
        if check:
            for box in data:
                check=char_check(box,box)
                if (box["left"] >= main_box["left"] and box["right"] <= main_box["right"] and box["top"] >= main_box["top"] and box["bottom"] <= main_box["bottom"]) and (box!=main_box) and check:
                    boxes_inside_container.append(box)
                    if main_box not in boxes_inside_container:
                        boxes_inside_container.append(main_box)
                    #print(f"this {box} is inside the {main_box} \n")
                elif not check:
                    break
            if boxes_inside_container:
                data=removing_found_items(data,boxes_inside_container)
                #print(f" removing_found_items is {boxes_inside_container} \n")
                temp__=combine_dicts(boxes_inside_container)
                #print(f"adding  box is {temp__}")
                data.append(temp__)
        else:
            continue
    return data



#--------------------------------------------------------------------------------------------------

# calling function with input ocrword data
# data_ocr=get_ocr_block(data)