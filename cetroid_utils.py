import math
import re
from collections import defaultdict
import difflib
# from gensim.models import Word2Vec
import os
import json
import ast
from db_utils import DB
import joblib
import numpy as np
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.model_selection import train_test_split
from sklearn.svm import LinearSVC
from sklearn.metrics import accuracy_score, classification_report
from fuzzywuzzy import fuzz
from app.svm_functions import *



db_config = {
    'host': os.environ['HOST_IP'],
    'port': os.environ['LOCAL_DB_PORT'],
    'user': os.environ['LOCAL_DB_USER'],
    'password': os.environ['LOCAL_DB_PASSWORD'],
}

#  this file consists of all the files which are used in the cetroid_api route
# function to find the y_tilt values 
def sublist_with_smallest_second_element(input_list):
    if not input_list:
        return None
    min_sublist = min(input_list, key=lambda x: x[1]) 
    return min_sublist


def find_ytilt(dict_):
    for key,list_ in dict_.items():
        result_list = sublist_with_smallest_second_element(list_)
        dict_[key]=result_list[0]
    return dict_

## function to find the x_tilt values 

# x_tilt
# def find_xtilda_within_threshold(input_list):
#     if len(input_list) < 2:
#         return None  # Return None if there are fewer than 2 sublists
#     # # Calculate the initial threshold as the difference between the second values of the first two sublists 
#     initial_threshold = abs(input_list[1][1] - input_list[0][1]) 
#     initial_threshold=initial_threshold+5 
#     result = [] 
#     for i in range(1, len(input_list)):  
#         current_diff = input_list[i][1] - input_list[i - 1][1]  
# #         print(current_diff)  
#         if current_diff > initial_threshold:   
#             result.append(input_list[i - 1][0])   
#             break # Stop when the difference exceeds the threshold  
#         result.append(input_list[i - 1][0]) 
#     return result


def find_distance(x1,y1,x2,y2):
    distance = math.sqrt((x2 - x1)**2 + (y2 - y1)**2)
    return round(distance, 2)


def remove_all_except_al_num(text):
    to_return = re.sub('[:,.!@#$%^&*(){}\_\-=`~\'";<>/?0-9]', ' ', text.lower())
    to_return = to_return.strip()
    return to_return

def eliminate_keys_with_digits_x_tilt(input_dict):
    # Create a new dictionary to store the filtered key-value pairs
    filtered_dict = {}
    
    # Iterate over the keys and values in the input dictionary
    for key, value in input_dict.items():
        # Check if the key contains only alphabetic characters (no digits)
#         print(f"########## key got is {key}")
        clear_key=remove_all_except_al_num(key)
#         print(f" ########## key after removing is {clear_key}")
        if clear_key.replace(" ", "").isalpha():
            # Add the key-value pair to the filtered dictionary
            # filtered_dict[clear_key] = value
            ## im doing this to maintain the integrity of key eg :: "Ba Amount Figures": [{"4 Ba Amount Figures:": [73.5,315.5]
            change_key=value[0]
            old_key = key
            new_key = clear_key
            # Replace the old key with the new key
            change_key[new_key] = change_key.pop(old_key)
            filtered_dict[clear_key] = value
    
    return filtered_dict

def eliminate_keys_with_digits(input_dict):
    # Create a new dictionary to store the filtered key-value pairs
    filtered_dict = {}
    
    # Iterate over the keys and values in the input dictionary
    for key, value in input_dict.items():
        # Check if the key contains only alphabetic characters (no digits)
#         print(f"########## key got is {key}")
        clear_key=remove_all_except_al_num(key)
#         print(f" ########## key after removing is {clear_key}")
        if clear_key.replace(" ", "").isalpha():
            # Add the key-value pair to the filtered dictionary
            filtered_dict[clear_key] = value
    
    return filtered_dict

def remove_all_except_al_num_prob_ele(text):
    to_return = re.sub('[:,.!@#$%^&*()\_\-=`~\'";<>/? ]|', '', text)
    to_return = to_return.strip()
    return to_return


# def get_prob_ele(elements__, x_corrd_thr=200.0, y_corrd_thr=40.0,x_tilt_find=False):
#     """
#     Get probable elements from an OCR output
#     :param elements: OCR output merged
#     :param x_corrd_thr: threshold for tilt in x axis
#     :param y_corrd_thr: threshold for tilt across y axis
#     :return: dictionary of elements and probable values
#     """

#     ele_dict = {'y_tilt':{},'x_tilt':{}}
#     y_tilt={}
#     x_tilt={}
#     special_chars=[":",",",".","!","@","#","$","%","^","&","*","(","()",")","_","-","=","`","~",";","<",">","/","?"]
#     print(f"elements are {elements__}")
#     for j, e in enumerate(elements__):  # parse through all the values
        
#         main_word=e['text']
#         main_word=remove_all_except_al_num_prob_ele(main_word)
#         if not main_word.isnumeric():  # Get only alphanumeric values as key
#             # probable_elements = []
# #             print(f" ######## boundingBox isss {e['boundingBox']}")
# #             e_coord = [int(c) for c in e['boundingBox'].split(',')]
#             e_coord = [int(c) for c in e['boundingBox']]
#             # Get centroids
#             e_cetroid_x = (e_coord[0]+e_coord[2])/2
#             e_cetroid_y = (e_coord[1]+e_coord[3])/2
# #             print(f" ########### mid point for {e_text} is x {e_cetroid_x} and y is {e_cetroid_y}")
# #             print(f" #########  j is {j} {elements[j+1:]}")
#             # for k, n_e in enumerate(elements__[j+1:]):  # OCR is from left, for each element get all the next ones
# #                 n_e_coord = [int(c) for c in n_e['boundingBox'].split(',')]
#             for k, n_e in enumerate(elements__): ## we used to check the left ones but now check with all the OCR data
#                 main_prob_word=n_e['text']
#                 if main_prob_word not in special_chars:
#                     n_e_coord = [int(c) for c in n_e['boundingBox']]
#                     # Get centroid for probable values
#                     ne_cetroid_x =( n_e_coord[0] + n_e_coord[2]) / 2
#                     ne_cetroid_y = (n_e_coord[1] + n_e_coord[3])/ 2
#     #                 print(f"####### text for {e_text} and n_e text is {n_e['text']} \n and ne_cetroid_x is {ne_cetroid_x} \n and ne_cetroid_y is {ne_cetroid_y} ")
#                     # if centroid difference less than threshold select as probable value
#     #                 print(f" ######## check ecords1  for {n_e['text']} {e_coord[1]} and {n_e_coord[1]}")
#     #                 print(f" ########## {n_e['text']} for cetnroed is {abs(ne_cetroid_x - e_cetroid_x)}")
#                     if abs(ne_cetroid_y - e_cetroid_y) < y_corrd_thr: 
#                         # print(f"######## y  tilt data got is {y_corrd_thr}")
#                         if e_coord[0] < n_e_coord[0]:
#                             dist=find_distance(e_cetroid_x,e_cetroid_y,ne_cetroid_x,ne_cetroid_y)
#         # choose the least distance among the distance and consider it as the final value for that keyword 
#                             # print(f" for less Y_coord_thr text is {e['text']} for {n_e['text']} with x cetroid is {ne_cetroid_x} y cetroid is {ne_cetroid_y} and distance is {dist}")
#                             if e['text'] not in y_tilt:
#                                 y_tilt[e['text']]=[[n_e['text'],dist]]
#                             else:
#                                 y_tilt[e['text']].append([n_e['text'],dist])
#     #                         ele_dict[e['text']].update([dist])
                            
#                     elif abs(ne_cetroid_x - e_cetroid_x) < x_corrd_thr:
#                     # elif abs(e_coord[0]- n_e_coord[0]) < x_corrd_thr:
#     #                     print(f" ######## elif of {n_e['text']}")
#                         prob_word=n_e['text']
#                         main_word_cords=[e_cetroid_x,e_cetroid_y]
#                         prob_word_cords=[ne_cetroid_x,ne_cetroid_y]
#                         if e_coord[1] < n_e_coord[1]:
#                             # print(f" text is {e['text']} for {n_e['text']} with x cetroid is {ne_cetroid_x} y cetroid is {ne_cetroid_y}")
#     #                         print(f"######## x  tilt data got is ")
#                             vertical_spacing=find_distance(e_cetroid_x,e_cetroid_y,ne_cetroid_x,ne_cetroid_y)
#     #     multiple lines : if M is given then take the M lines with least y distance and make it as value 
#     #  if not M , then take all the lines with the delta y2  > 1.5 times delta y1
#     #                         print(f"  for less x_coord_thr text is {e['text']} for {n_e['text']} with x cetroid is {ne_cetroid_x} y cetroid is {ne_cetroid_y} and dist is {dist}")
#                             if main_word not in x_tilt:
#     #                             x_tilt[e['text']]=[[n_e['text'],ne_cetroid_y]]
#                                 x_tilt[main_word]=[{main_word:main_word_cords}]
#     #                               x_tilt[main_word].append({main_word:main_word_cords})
#                                 x_tilt[main_word].append({prob_word:prob_word_cords})
#                             else:
#                                 x_tilt[main_word][1].update({prob_word:prob_word_cords})
#     #                         ele_dict[e['text']].update([n_e['text'],ne_cetroid_y])
#     #                         ele_dict[e['text']].update([dist])
#                     else:
#     #                     print(f" ######## text got in else is {e['text']} with {n_e['text']}")
#                         continue
#                 else:
#                     pass
#     elements__=[]
#     return x_tilt,y_tilt



def get_prob_ele(elements__, x_corrd_thr, y_corrd_thr,x_tilt_find=True):
    """
    Get probable elements from an OCR output
    :param elements: OCR output merged
    :param x_corrd_thr: threshold for tilt in x axis
    :param y_corrd_thr: threshold for tilt across y axis
    :return: dictionary of elements and probable values
    """
    print(F"################## ythrush got is {y_corrd_thr}")
    ele_dict = {'y_tilt':{},'x_tilt':{}}
    y_tilt={}
    x_tilt={}
    special_chars=[":",",",".","!","@","#","$","%","^","&","*","(","()",")","_","-","=","`","~",";","<",">","/","?"]
#     print(f"elements are {elements__}")
    for j, e in enumerate(elements__):  # parse through all the values
        
        main_word=e['text']
        main_word=remove_all_except_al_num_prob_ele(main_word)
        if not main_word.isnumeric():
            e_coord = [int(c) for c in e['boundingBox']]
            
            e_cetroid_x = (e_coord[0]+e_coord[2])/2
            e_coord_left=(e_cetroid_x+e_coord[0])/2
            e_coord_right=(e_cetroid_x+e_coord[2])/2
            e_cetroid_y = (e_coord[1]+e_coord[3])/2
            
            for k, n_e in enumerate(elements__):
                main_prob_word=n_e['text']
                if main_prob_word not in special_chars and e['text']!=n_e['text']:
                    n_e_coord = [int(c) for c in n_e['boundingBox']]
                    ne_cetroid_x =( n_e_coord[0] + n_e_coord[2]) / 2
                    ne_cetroid_y = (n_e_coord[1] + n_e_coord[3])/ 2
                    
#                     print(f"####### text y tilt for {e} and n_e text is {n_e['text']} \n and ne_cetroid_x is {ne_cetroid_x} \n and ne_cetroid_y is {ne_cetroid_y} {(e_coord[1]-n_e_coord[1])}")
                    if abs(e_coord[1]-n_e_coord[1])<y_corrd_thr and (e_coord[2]<n_e_coord[0]):
                    #here the thrushold should be according to the document type app=15 and remian=7
#                         print(f"####### text y tilt for {e} and n_e text is {n_e['text']} \n and ne_cetroid_x is {ne_cetroid_x} \n and ne_cetroid_y is {ne_cetroid_y} and {(e_coord[1]-n_e_coord[1])} ")
                        dist=abs(e_coord[0]-n_e_coord[2])
                        if e['text'] not in y_tilt:
                            y_tilt[e['text']]=[[n_e['text'],dist]]
                        else:
                            y_tilt[e['text']].append([n_e['text'],dist])
                            
                    elif (e_coord[1]-n_e_coord[1])<5 and (( n_e_coord[0] < e_cetroid_x < n_e_coord[2]) or ( n_e_coord[0] < e_coord[0] < n_e_coord[2]) or ( n_e_coord[0] < e_coord[2] < n_e_coord[2]) or ( n_e_coord[0] < e_coord_right < n_e_coord[2]) or ( n_e_coord[0] < e_coord_left < n_e_coord[2])):
#                         print(f"####### text x titl for {e} and n_e text is {n_e['text']} \n and ne_cetroid_x is {ne_cetroid_x} \n and ne_cetroid_y is {ne_cetroid_y} ")
                        vertical_spacing=abs(e_coord[1]-n_e_coord[3])
                        
                        if e['text'] not in x_tilt:
                            
                            x_tilt[e['text']]=[[n_e['text'],vertical_spacing]]
                        else:
                            x_tilt[e['text']].append([n_e['text'],vertical_spacing])
#                         print(x_tilt)
                    else:
                        continue
                else:
                    pass
    elements__=[]
    return x_tilt,y_tilt



def find_xtilda_within_threshold(input_list):
    if len(input_list) < 1:
        return input_list[0] 
    else:
        result = min(input_list, key=lambda x: x[1])
        return result[0]


def generate_x_y_tilt_words_parse(x_tilt,y_tilt,x_tilt_find):

    x_tilt_values={}
    if x_tilt_find:
        for k, value in x_tilt.items():
        #     print(f" ###### i/p x_tilt feedingn values is {value}")
            x_tilt_val= find_xtilda_within_threshold(value)
        #     print(f" ######### o/p x_tilt_val is {x_tilt_val}")
            if x_tilt_val:
                # x_tilt_full_val=' '.join(x_tilt_val)
                if k not in x_tilt_values:
                    # x_tilt_values[k]=x_tilt_full_val
                    x_tilt_values[k]=x_tilt_val
            else:
                pass
    
    y_tilt_values = find_ytilt(y_tilt)
    
    return x_tilt_values,y_tilt_values


# partA approach not using it currently
def update_ocr(full_train_data,x_tilt_values,y_tilt_values):
    update_ocr_data={}
    for field_name,field_train_data in full_train_data.items():
        for ftd in field_train_data:
            clear_key=remove_all_except_al_num(ftd['keyword'].lower())
            get_tilt=ftd['tilt']
    #         print(f" ########### clear key is {clear_key}")
            if get_tilt=='left' or get_tilt=='right':
                if clear_key in y_tilt_values:
                    get_val=y_tilt_values[clear_key]
                    if get_val:
                        update_ocr_data[field_name]=get_val
                        print(f"  !!!! yayyyyyy!!!!!!!!!!!!! value got is {get_val}")
                    break
            elif get_tilt=='bottom':
                if clear_key in x_tilt_values:
                    get_val=x_tilt_values[clear_key]
                    if get_val:
                        update_ocr_data[field_name]=get_val
                        print(f"  !!!! yayyyyyy!!!!!!!!!!!!! value got is {get_val}")
                    break
            else:
    #             print(f" ########### shittt didn't get keyword also")
                pass
    print(f" ####### update ocr is {update_ocr_data}")
    return update_ocr_data


# part B approach uis
def find_key_algo(word_to_check,data):
    print(word_to_check,data)
    word_to_check=word_to_check.lower()
    # Extract keywords from each word dictionary
    most_similar_word=''
    keywords = data
    sentences=data.values()
    # for word, sub_dict_list in data.items():
    #     extracted_keywords = [sub_dict["keyword"] for sub_dict in sub_dict_list]
    #     keywords[word] = extracted_keywords
    #     sentences.append(extracted_keywords)

    # Assuming you have a list of sentences as 'sentences'
    # word2vec = Word2Vec(sentences, vector_size=100, window=5, min_count=1, sg=1)


    # Assuming you have already trained a Word2Vec model named 'word2vec'
    # Assuming you have already trained a Word2Vec model named 'word2vec'

    # Check if a word is in the vocabulary
    # if word_to_check in word2vec.wv.key_to_index:
    #     most_similar_word=word_to_check
    #     print(f"'{word_to_check}' is in the vocabulary.")
    # else:
    
    # print(f"so lets find word using similarity")
    # word2vec_vocab = list(word2vec.wv.key_to_index.keys())
    similarities = {}

    for word_l in sentences:
        for word in word_l:
            word_=remove_all_except_al_num_prob_ele(word)
            word_=word_.lower()
            seq = difflib.SequenceMatcher(None, word_to_check, word_)
            similarity = seq.ratio()  # Ratio of similarity between the two words
            similarities[word] = similarity

    # Sort the similarities by descending order
    sorted_similarities = sorted(similarities.items(), key=lambda x: x[1], reverse=True)
    print(f" ##### the similarity is {sorted_similarities[0][1]}")
    if sorted_similarities[0][1]>=0.75:
        most_similar_word = max(similarities, key=similarities.get)
        # print(most_similar_word)
    else:
        return 
    field_name=''
    for category, sub_dict_list in data.items():
        for sub_dict in sub_dict_list:
            # if sub_dict["keyword"] == most_similar_word:
            if sub_dict.lower() == most_similar_word.lower():
                # tilt_value = sub_dict["tilt"]
                field_name=category
                # print(f"Tilt for {word_to_check} of  '{category}':")
                return field_name
            else:
                pass
    # return tilt_value,field_name



def find_value_tilt(get_tilt,key,category,x_tilt,y_tilt,update_ocr_data,api,x_tilt_values,y_tilt_values):
    print(f"get tilt is {get_tilt}")
    if api==2:
        return update_ocr_data
    if get_tilt=='left' or get_tilt=='right':
        try :
            get_val=y_tilt_values[key]
        except:
            print(f"right is not found")
            return find_value_tilt('bottom',key,category,x_tilt,y_tilt,update_ocr_data,api+1,x_tilt_values,y_tilt_values)
        if get_val:
            update_ocr_data[category]=get_val
            print(f"value got is {get_val}")
    elif get_tilt=='bottom':
        try:
            get_val=x_tilt_values[key]
        except:
            print(f"bottom is not found")
            return find_value_tilt('left',key,category,x_tilt,y_tilt,update_ocr_data,api+1,x_tilt_values,y_tilt_values)
        if get_val:
            update_ocr_data[category]=get_val
            print(f"value got is {get_val}")
    return update_ocr_data

# def generate_x_y_tilt_words_parse(x_tilt,y_tilt,x_tilt_find):
#     # print(f" ###### x tilt is {x_tilt}")

#     # print(f" ########## y tilt is {y_tilt}")

#     # print(f" ############ x tilt without alpha removal {x_tilt}")

#     x_tilt_char_keys=eliminate_keys_with_digits_x_tilt(x_tilt)
#     y_tilt_char_keys=eliminate_keys_with_digits(y_tilt)
#     # print(f" ########## x tilt is with alpha removal {x_tilt_char_keys}")
#     # print(f" ############ y tilt after alpha removal is {y_tilt_char_keys}")



#     # print(f" ############ y_tilt values got is {y_tilt_values}")
    
#     x_tilt_values={}
#     if x_tilt_find:
#         for k, value in x_tilt_char_keys.items():
#         #     print(f" ###### i/p x_tilt feedingn values is {value}")
#             x_tilt_val= find_xtilda_within_threshold(value)
#         #     print(f" ######### o/p x_tilt_val is {x_tilt_val}")
#             if x_tilt_val:
#                 # x_tilt_full_val=' '.join(x_tilt_val)
#                 if k not in x_tilt_values:
#                     # x_tilt_values[k]=x_tilt_full_val
#                     x_tilt_values[k]=x_tilt_val[:]
#             else:
#                 pass

#     # print(f" ######### final x_tilt_value are {x_tilt_values}")

#     y_tilt_values = find_ytilt(y_tilt_char_keys)
    
#     return x_tilt_values,y_tilt_values


def merge_and_get_unique(x_tilt_values,x_tilt_values_wrds,y_tilt_values,y_tilt_values_wrds):
    # Merge the dictionaries
    merged_dict_x = {**x_tilt_values, **x_tilt_values_wrds}
    merged_dict_y= {**y_tilt_values, **y_tilt_values_wrds}

    return merged_dict_x,merged_dict_y

def remove_all_except_al_num_lwr(text):
    to_return = re.sub('[:,.!@#$%^&*()\_\-=`~\'";<>/?]', '', text)
    to_return = to_return.strip()
    return to_return


def remove_from_ocr(remove_words,ocr_data_parse):
    ocr_data_deleted=[]
    for word in remove_words:
        for ocr_data in ocr_data_parse:
            if word != ocr_data['word']:
                # print(f"#### matched word is {word}m###### text is {ocr_data['word']}")
                if ocr_data not in ocr_data_deleted:
                    ocr_data_deleted.append(ocr_data)
    return ocr_data_deleted



def get_ocr_frmt_data(ocr_blk_data):
    ocr_data_blk_frmt=[]
    for ocr_data in ocr_blk_data:
        ocr_data_blk_frmt.append({'text':ocr_data['word'],'boundingBox':[ocr_data['left'],ocr_data['top'],ocr_data['right'],ocr_data['bottom']]})
        return ocr_data_blk_frmt
    
"""
def predict_labels_svm(word_list, file_type):
    try:
        print(f" ####################  predict_labels_svm fn, word list got {word_list} \n")
        if len(word_list) == 0:
            # print(f"Input word list is empty ")
            return {}

        # Load the SVM model and CountVectorizer
        file_type = file_type.lower().replace(" ", "_")
        print(f"file type got is in svm is {file_type}")

        # Load the SVM model and CountVectorizer
        model_filename = f'/var/www/prediction_api/app/data/model_files/{file_type}_corpus_svm_classifier_model.joblib'
        vectorizer_filename = f'/var/www/prediction_api/app/data/model_files/{file_type}_corpus_count_vectorizer.joblib'
        # print(f"looking for mode files {model_filename},{vectorizer_filename}")
        
        try:
            loaded_model = joblib.load(model_filename)
            loaded_vectorizer = joblib.load(vectorizer_filename)
            # print(f"loaded svm model {loaded_model}, {loaded_vectorizer}")
        except FileNotFoundError:
            # print(f"SVM model or CountVectorizer not found for file type: {file_type}")
            return {}  # Return an empty dictionary if files are not found

        # Convert all words to lowercase
        word_list_lowercase = [word.lower() for word in word_list]

        # List of words for inference
        word_list_vectorized = loaded_vectorizer.transform(word_list_lowercase)

        # Make predictions on the preprocessed data
        predicted_classes = loaded_model.predict(word_list_vectorized)

        # Optionally, obtain confidence scores or decision function values
        decision_function_values = loaded_model.decision_function(word_list_vectorized)

        # Initialize an output dictionary to store label-word pairs
        output_dict = {}

        # Iterate through the predicted classes and confidence scores
        for original_word, predicted_class, confidence_scores in zip(word_list, predicted_classes, decision_function_values):
            # Get the confidence score for the predicted label
            label_confidence = max(confidence_scores)

            # Check if the label is not in the output dictionary, add it with an empty list
            if predicted_class not in output_dict:
                output_dict[predicted_class] = []

            # Append a sublist containing the original word and its confidence to the list for the label
            output_dict[predicted_class].append([original_word, label_confidence])

        # Print the output dictionary
        # print(f"output_dict before cleaning for labels from svm is {output_dict}")

        # Create a new dictionary to store the result
        result_dict = {}

        for label, word_confidence_list in output_dict.items():
            # Skip 'other' label
            if label == 'other':
                continue

            # Sort the word_confidence_list by confidence in descending order
            sorted_list = sorted(word_confidence_list, key=lambda x: x[1], reverse=True)

            # For other labels, take the original word with the highest confidence
            if sorted_list:
                result_dict[label] = sorted_list[0][0]

        # Print the new dictionary
        print(f" ################ result_dict after svm is {result_dict} \n")
        return result_dict
    except Exception as e:
        print(f"Error occurred in predict_labels_svm, returned an empty dict, error is {e}")
        return {}
"""



# from the corresponding values finalize a value based on the x centroid of key and value  
def finalize_values(corresponding_values,key_cords):
    x_centoid_val=[]
    x_val=[]
    # print(f" ###### corresponding_values are {corresponding_values} and key_cords are {key_cords}")
    for values in corresponding_values:
        for val, val_cords in values.items():
            x_centoid_val.append(val_cords[0])
            x_val.append(val)
    closest_value = min(x_centoid_val, key=lambda x: abs(x - key_cords))
    idx=x_centoid_val.index(closest_value)
    val=x_val[idx]
    # print(f"### val is {val}")
    return val

### new way to find the x_tilt_values

# Function to find the lowest y centroid value
def find_lowest_value_within_margin(data,margin):
    new_data={} 
    for key,values in data.items():
        try:
            corresponding_values = []
            lowest_value = float("inf")
    #         print(f" ######## item got is {values[0]}")
            for value in values[1:]:
    #             print(f" ##### value is {value}")
                key_x_centroid=values[0][key][0]
                key=key
                for val,cords in value.items():
    #                 print(f" #### val is {val} and cords is {cords} and y cord is {cords[1]}")
                    val_y_centroid=cords[1]
                    y_cords=cords[1]
                    if val_y_centroid < lowest_value:
                        lowest_value = val_y_centroid
    #                     print(f" ########## current lowest value is {lowest_value}")
                    threshold=lowest_value+margin
    #                 print(f"##### current lowest threshold is {threshold}")
                    if val_y_centroid==lowest_value or val_y_centroid<=threshold:
                        corresponding_values.append({val:cords})

            final_val=finalize_values(corresponding_values,key_x_centroid)
            new_data[key]=final_val
        except Exception as e:
            # print(f"########## may b key exception has occured but im continuing it")
            continue
    return new_data

### layout LM Function
# Calculate the distance between two bounding boxes
def calculate_distance(box1, box2):
    left1, top1, right1, bottom1 = box1
    left2, top2, right2, bottom2 = box2
    x1 = (left1 + right1) / 2
    y1 = (top1 + bottom1) / 2
    x2 = (left2 + right2) / 2
    y2 = (top2 + bottom2) / 2
    return ((x1 - x2) ** 2 + (y1 - y2) ** 2) ** 0.5

def find_nearest_words_in_ocr(input_dict, ocr_data, json_files):
    found_words = {} # Dictionary to store found words and their info
    lowest_distance=float('inf')
    for json_file in json_files:
    # Load the JSON file and create a list of words
        with open(json_file, 'r', encoding='utf-8') as file:
            words_list = json.load(file)
            for word in words_list:
                found_word_data = None # Stores the data for the found word
                for page in ocr_data:
                    for item in page:
                        ocr_word = item["word"]
                        similarity = fuzz.ratio(word.lower(), ocr_word.lower())
                        if similarity >= 90:
                            found_word_data = {
                            "word": ocr_word,
                            "box": [item["left"], item["top"], item["right"], item["bottom"]],
                            "neighborhood_words": []
                            }
                            break
                        if found_word_data:
                            break

                    if found_word_data:
                        # Find neighborhood words
                        for label, words_list in input_dict.items():
                            for words_dict in words_list:
                                for input_word, input_box in words_dict.items():
                                    distance = calculate_distance(found_word_data["box"], input_box)
                                    if distance < lowest_distance:
                                        lowest_distance=distance
#                                         found_word_data["neighborhood_words"].append({input_word: input_box})
                                        found_word_data["neighborhood_words"]={input_word: input_box}

                        # Add the file name as a key in the found_words dictionary
                        file_name = json_file.split('.')[0] # Remove the file extension
                        if file_name not in found_words:
                            found_words[file_name] = []
                            found_words[file_name].append(found_word_data)

        return found_words
    
def map_neighbour_dict(found_words,input_dict):
    key_val_map={}
    for lable,lable_data in found_words.items():
        neighbour_data=lable_data[0]['neighborhood_words']
        for value,bbox in neighbour_data.items():
            for fld,fld_data in input_dict.items():
                for fld_val in fld_data:
                    for val,bbox in fld_val.items():
                        if value==val:
                            print(f" #### lable is {lable} and fld is {fld}")
                            new_label=lable+fld.split('_')[1]
                            key_val_map[new_label]=value
    return key_val_map