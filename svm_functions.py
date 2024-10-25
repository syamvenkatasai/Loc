import joblib
import json
from fuzzywuzzy import fuzz
import math
import re
import json
import numpy as np
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.model_selection import train_test_split
from sklearn.svm import LinearSVC  
from sklearn.metrics import accuracy_score, classification_report
import joblib  

from fuzzywuzzy import fuzz
import re

def predict_with_svm_old(word_list, file_type, ocr_data,api, input_dict=None):
    try:
        if not word_list:
            return {}, {}
        print(f" ##### PREDICT WITH SVM IS word list is  \n ocr_data is ")
        file_type = file_type.lower().replace(" ", "_")
        model_filename = f'/var/www/prediction_api/app/data/model_files/{file_type}_{api}_corpus_svm_classifier_model.joblib'
        vectorizer_filename = f'/var/www/prediction_api/app/data/model_files/{file_type}_{api}_corpus_count_vectorizer.joblib'

        try:
            loaded_model = joblib.load(model_filename)
            loaded_vectorizer = joblib.load(vectorizer_filename)
        except FileNotFoundError:
            return {}, {}

        word_list_vectorized = loaded_vectorizer.transform(word.lower() for word in word_list)
        predicted_classes = loaded_model.predict(word_list_vectorized)
        output_dict = {}
        single_value_dict = {}
        multiple_value_dict = {}

        for original_word, predicted_class in zip(word_list, predicted_classes):
            if predicted_class != 'other':
                output_dict.setdefault(predicted_class, []).append(original_word)

            if 'address' in predicted_class.lower() or 'name' in predicted_class.lower():
                multiple_value_dict.setdefault(predicted_class, []).append(original_word)
            else:
                label_count = predicted_classes.tolist().count(predicted_class)
                if label_count == 1:
                    single_value_dict.setdefault(predicted_class, []).append(original_word)

        # Convert single_value_dict values to a single list
        for key, value in single_value_dict.items():
            single_value_dict[key] = value  # No need for an extra list

        # Add invoice_name to single_value_dict if it exists
        if 'invoice_name' in output_dict:
            single_value_dict['invoice_name'] = output_dict['invoice_name']

        multiple_value_dict = {k: v for k, v in multiple_value_dict.items() if v}
        multiple_value_dict.pop('other', None)
       
        new_dict = {}  

        
            
            # for label, words in input_dict.items():
            #     if not ('address' in label.lower()):
            #         if label in multiple_value_dict:
            #             multiple_value_dict[label].extend(words)
            #         else:
            #             multiple_value_dict[label]=words
        for label, words in multiple_value_dict.items():
            label_data = []  # Initialize a list for this label
            for word in words:
                # Search for the word in the OCR data and retrieve its box
                box = None
                for item in ocr_data:
                    if item["word"] == word:
                        box = [item["left"], item["top"], item["right"], item["bottom"]]
                        break
                # Add {word: box} to the label_data list
                if box is not None:
                    label_data.append({word: box})
                else:
                    label_data.append({word: []})
            # Add the label_data list to the new_dict with the label as the key
            new_dict[label] = label_data
        print(F"out dict is {new_dict}")
        if input_dict is not None:
            new_dict.update(input_dict)

        # Print the new_dict
        
        print(f"single_value_dict being returned in predict labels with svm (values) is {single_value_dict}")
        print(f"multi_value_dict being returned in predict labels with svm (values) is {new_dict}")
        
        return single_value_dict, new_dict

    except Exception as e:
        print(f"Error occurred, returned empty dictionaries, error is {e}")
        return {}, {}

def process_sublists(input_list):
    # Initialize an empty list to store the modified sublists
    modified_list = []

    # Initialize variables to store the previous confidence and string
    prev_confidence = None
    prev_string = None

    for sublist in input_list:
        string, confidence = sublist

        # Process confidence values
        # if prev_confidence is not None and abs(prev_confidence - confidence) > 0.1:
            
        #     continue  

        
        string_no_spaces = ''.join(e for e in string if e.isalnum())  # Remove spaces and special characters
        prev_string_no_spaces = ''.join(e for e in prev_string if e.isalnum()) if prev_string else None

        if prev_string_no_spaces is not None and (
            string_no_spaces == prev_string_no_spaces or
            string_no_spaces in prev_string_no_spaces or
            prev_string_no_spaces in string_no_spaces
        ):
            # Skip this sublist if the strings are the same or one is a substring of the other
            continue

        # If the sublist passes all checks, add it to the modified list
        modified_list.append(sublist)

        # Update the previous confidence and string
        prev_confidence = confidence
        prev_string = string

    return modified_list


def predict_with_svm(word_list, file_type, ocr_data,ocr_ref,api,tenant_id,seg):
    try:
        if not word_list:
            return {}, {}
        print(f" ##### PREDICT WITH SVM IS word list is {word_list} \n ocr_data is {ocr_data}")
        file_type = file_type.lower().replace(" ", "_")
        model_filename = f'/var/www/extraction_api/app/extraction_folder/non_table_fields/non_table_fields_{api}_logistic_regression_model.joblib'
        vectorizer_filename = f'/var/www/extraction_api/app/extraction_folder/non_table_fields/non_table_fields_{api}_count_vectorizer.joblib'
        print(f"file_paths are {model_filename},{vectorizer_filename}")

        try:
            loaded_model = joblib.load(model_filename)
            loaded_vectorizer = joblib.load(vectorizer_filename)
        except:
            print(F"returning file not found")
            return {}

        word_list_vectorized = loaded_vectorizer.transform(word.lower() for word in word_list)     
      
          
        predicted_classes = loaded_model.predict(word_list_vectorized)

            
        decision_function_values = loaded_model.decision_function(word_list_vectorized)

            
        output_dict = {}

        print(f"got words list in predict_with_svm")
#         
        output_dict = {}
        
        multiple_value_dict = {}
        
        for word, predicted_class, confidence_scores in zip(word_list, predicted_classes, decision_function_values):
            # Get the confidence score for the predicted label
            # label_confidence = max(confidence_scores)
            try:
                label_confidence = max(confidence_scores)
            except:

                if not isinstance(confidence_scores, list):
                    confidence_scores = [confidence_scores]

                if any(confidence_scores):
                    label_confidence = max(confidence_scores)
                # label_confidence = max(confidence_scores)

            # Check if the label is not in the output dictionary, add it with an empty list
            if predicted_class not in output_dict:
                output_dict[predicted_class] = []

            # Append a sublist containing the word and its confidence to the list for the label
            output_dict[predicted_class].append([word, label_confidence])

            # Print the output dictionary
#         print(f"output_dict before cleaning for labels from svm is {output_dict}")
        
        result_dict = {}
        multiple_values_dict={}

        for label, word_confidence_list in output_dict.items():            
            if label == 'other':
                continue
            # Sort the word_confidence_list by confidence in descending order
            sorted_list = sorted(word_confidence_list, key=lambda x: x[1], reverse=True)

            # if 'name' not in label.lower() and 'address' not in label.lower():
            #     # Pick the first value and update it to result_dict
            #     result_dict[label] = sorted_list[0][0]
            if label.lower().find('address') != -1 or label.lower().find('name') != -1:
                list_item=process_sublists(sorted_list)
                multiple_values_dict[label]=[list_item[i][0] for i in range(len(list_item))]
                # multiple_values_dict[label] = [sorted_list[i][0] for i in range(len(sorted_list))]
            else:
                # For other labels, process them as before
                list_item = process_sublists(sorted_list)
                if len(list_item) == 1:
                    result_dict[label.lower()] = list_item[0][0]
                else:
                    multiple_values_dict[label] = [list_item[i][0] for i in range(len(list_item))]                    
        
        # updated_multiple_value_dict={}
        # for label, words in multiple_values_dict.items():
        #     label_data = []  # Initialize a list for this label
        #     for word in words:
        #         # Search for the word in the OCR data and retrieve its box
        #         box = None
        #         for item in ocr_data:
        #             if item["word"] == word:
        #                 box = [item["left"], item["top"], item["right"], item["bottom"]]
        #                 break
        #         # Add {word: box} to the label_data list
        #         if box is not None:
        #             label_data.append({word: box})
        #         else:
        #             label_data.append({word: []})
        #     # Add the label_data list to the new_dict with the label as the key
        #     updated_multiple_value_dict[label] = label_data

        print(f"output dict with thresholds is {output_dict}")
        
        print(F"multiple value dict befor callins layout fn is in predict with svm {multiple_values_dict}")
        
        # final_labels=get_layout_labels(file_type,updated_multiple_value_dict,ocr_ref,tenant_id)
        # print(final_labels)
        # result_dict.update(final_labels)
        print(f" result dict returned in predict labels with svm (values) is {result_dict}")

        for lable,word_list in multiple_values_dict.items():
            out=find_values(word_list,lable)
            result_dict[lable.lower()]=out
        print(f"single_value_dict being returned in predict labels with svm (values) is {result_dict}")
        
        
        return result_dict
    except Exception as e:
        print(f"the error is {e}")
         # Use traceback.print_exc() to print the line number and error message
        return {}


def find_values(word_list,desired_label):

    print(f" recived list and lable for find_values is {word_list} -------------> {desired_label}")
    
    model_filename = f"/var/www/extraction_api/app/extraction_folder/non_table_fields/non_table_f_v_logistic_regression_model.joblib"
    vectorizer_filename = f"/var/www/extraction_api/app/extraction_folder/non_table_fields/non_table_f_v_count_vectorizer.joblib"
    try:
        # Load the model
        loaded_model = joblib.load(model_filename)

        # Load the vectorizer
        vectorizer = joblib.load(vectorizer_filename)
    except:
        print(f"######################### joblib file for this seg is missing {vectorizer_filename} and {model_filename}")
        return {}
    word_list_lower = [word.lower() for word in word_list]

    # Preprocess the list of words using the loaded vectorizer
    word_list_vectorized = vectorizer.transform(word_list_lower)

    # Get the probability estimates for the classes
    proba_estimates = loaded_model.predict_proba(word_list_vectorized)

    model_classes = loaded_model.classes_

    # Check if the desired label is in the model's classes
    if desired_label not in model_classes:
        print(f"Desired label '{desired_label}' not found in the model's classes.")
        return None

    # Find the index of the desired label in the model's classes
    desired_label_index = list(loaded_model.classes_).index(desired_label)

    # Initialize variables to track the best word and highest confidence score
    best_word = None
    highest_confidence = -1.0  # Initialize with a very low value

    # Iterate through the words and find the one with the highest confidence
    for i, (word, word_lower) in enumerate(zip(word_list, word_list_lower)):
        confidence_score = proba_estimates[i][desired_label_index]
#         logging.info(f"the word in find values word is {word} and confidence is {confidence_score}")
        if confidence_score > highest_confidence:
            best_word = word  # Return the original word with original case
            highest_confidence = confidence_score
#             logging.info(f"the word in find values word is {word} and highest_confidence is {highest_confidence}")

    # Return the word with the highest confidence for the given label
    print(f"output got from value (find_values) svm is {best_word} for {desired_label}")
    return best_word
    
# def calculate_relative_positions(input_dict):
#     output_dict = {}
#     # print(f"### CALCULATE RELATIVE POSITITONS INPUT IS {input_dict}")
#     for label, boxes_list in input_dict.items():
#         positions = {'top': [], 'bottom': [],'left':[],'right':[]}
#         sorted_boxes = sorted(boxes_list, key=lambda x: (list(x.values())[0][1], list(x.values())[0][0]))
#         if (len(sorted_boxes))==1:
#             # box1_key = list(sorted_boxes[0].keys())[0]
#             box1_key=list(sorted_boxes[0].keys())[0]
#             positions['top'].append(box1_key)
#             output_dict[label]=positions

#             # return output_dict
#         else:
#             for i in range(len(sorted_boxes)-1):
            
#                 current_key = list(sorted_boxes[i].keys())[0]
#                 next_key = list(sorted_boxes[i+1].keys())[0]
#                 current_box = sorted_boxes[i][current_key]
#                 next_box = sorted_boxes[i+1][next_key]
#                 print(f"current_items {current_key} next {next_key}")
#                 if abs(current_box[1]-next_box[1])<5:
#                     print("tops are same")
#                     if current_box[0]<next_box[0]:
#                         positions['left'].append(current_key)
#                         positions['right'].append(next_key)
                    

#                 else:
#                     positions['top'].append(current_key)
#                     positions['bottom'].append(next_key)


#             output_dict[label] = positions

#     output_dict = {outer_key: {inner_key: inner_value for inner_key, inner_value in inner_dict.items() if inner_value}
#                  for outer_key, inner_dict in output_dict.items()}

#     print(f"### CALCULATE RELATIVE POSITITONS output is {output_dict}")
    
#     return output_dict

def calculate_relative_positions(input_dict, return_dict):
    
    if len(input_dict)==0:
        print(f"return_dict is {return_dict}")
        return return_dict
    elif len(input_dict)==1:
        if not return_dict:
            for key,value in input_dict[0].items():
                return_dict[key]='top'
            return return_dict
        else:
            return return_dict
    else:
        sorted_list = sorted(input_dict, key=lambda x: (list(x.values())[0][1]))

        print(sorted_list)
        first_item, second_item = sorted_list[0], sorted_list[1]

        first_key, first_box = list(first_item.keys())[0], list(first_item.values())[0]

        second_key, second_box = list(second_item.keys())[0], list(second_item.values())[0]

        print(f"first top is {first_box[1]} second_top is {second_box[1]}")

        if abs(first_box[1]-second_box[1])<=5:
            if first_key in return_dict:
                return_dict[second_key]=return_dict[first_key]+'_right'
                return_dict[first_key]=return_dict[first_key]+'_left'

            else:
                return_dict[first_key]='left'
                return_dict[second_key]='right'
            sorted_list.pop(0)
            return calculate_relative_positions(sorted_list,return_dict)
        else:
            if first_key in return_dict:
                return_dict[second_key]=return_dict[first_key]+'_bottom'
                return_dict[first_key]=return_dict[first_key]+'_top'

            else:
                return_dict[first_key]='top'
                return_dict[second_key]='bottom'
            sorted_list.pop(0)
            return calculate_relative_positions(sorted_list,return_dict)

import json

def create_combined_dict_old(input_dict, pos_config_file):
    try:
        # file_type = file_type.lower().replace(" ", "_")
        # Load the position configuration from the JSON file
    
        with open(pos_config_file, 'r', encoding='utf-8') as file:
            pos_config = json.load(file)

        combined_dict = {}

        for label, positions in input_dict.items():
            for position, words in positions.items():
                # Combine position from JSON file and last part of label
                combined_key = pos_config.get(position, '') + label.split('_')[-1]
                
                # Add only the first word from the list
                if words:
                    word = words[0]
                    if combined_key not in combined_dict:
                        combined_dict[combined_key] = ""
                    combined_dict[combined_key] += word + " "  # Concatenate words as strings

        # Remove trailing space from combined values
        for key in combined_dict:
            combined_dict[key] = combined_dict[key].strip()
        print(f"output got in create combined_dict is {combined_dict}")
        return combined_dict

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return {}

def create_combined_dict(input_dict, pos_config_file):
    try:
        print("in CREATE_COMBINED_DICT input dict got is {input_dict}")
        with open(pos_config_file, 'r', encoding='utf-8') as file:
            pos_config = json.load(file)

        combined_dict = {}

        # for main_key, positions in input_dict.items():
        #     if positions in pos_config:
        #         position_info = pos_config[positions]
        #         for position, words in positions.items():
        #             if position in position_info:
        #                 target_key = position_info[position]
        #                 combined_dict[target_key] = words[0]  # Combine words into a single string
        print(f"pos_config is {pos_config}")
        for main_key, positions in input_dict.items():
            
            if main_key in pos_config:
                position_info = pos_config[main_key]
                for word,position in positions.items():
                    if position in position_info:
                        combined_dict[position_info[position]]=word

        return combined_dict
    except Exception as e:
        print(f"An error occurred: in create_combined_dict {str(e)}")
        return {}


# Calculate the distance between two bounding boxes
def calculate_distance(box1, box2):
    left1, top1, right1, bottom1 = box1
    left2, top2, right2, bottom2 = box2
    x1 = (left1 + right1) / 2
    y1 = (top1 + bottom1) / 2
    x2 = (left2 + right2) / 2
    y2 = (top2 + bottom2) / 2
    return ((x1 - x2) ** 2 + (y1 - y2) ** 2) ** 0.5

# def find_nearest_words_in_ocr(input_dict, ocr_data, json_files):
#     found_words = {} # Dictionary to store found words and their info
#     lowest_distance=float('inf')
#     for json_file in json_files:
#     # Load the JSON file and create a list of words
#         with open(json_file, 'r', encoding='utf-8') as file:
#             words_list = json.load(file)
#             print(f" ####### words_list got is {words_list}")
#             for word in words_list:
#                 found_word_data = None # Stores the data for the found word
#                 for item in ocr_data:
                    
#                     ocr_word = item["word"]
#                     similarity = fuzz.ratio(word.lower(), ocr_word.lower())
#                     if similarity >= 80:
#                         found_word_data = {
#                         "word": ocr_word,
#                         "box": [item["left"], item["top"], item["right"], item["bottom"]],
#                         "neighborhood_words": []
#                         }
#                         break
#                     if found_word_data:
#                         break

#                     if found_word_data:
#                         # Find neighborhood words
#                         for label, words_list in input_dict.items():
#                             for words_dict in words_list:
#                                 for input_word, input_box in words_dict.items():
#                                     distance = calculate_distance(found_word_data["box"], input_box)
#                                     if distance < lowest_distance:
#                                         lowest_distance=distance
# #                                         found_word_data["neighborhood_words"].append({input_word: input_box})
#                                         found_word_data["neighborhood_words"]={input_word: input_box}

#                         # Add the file name as a key in the found_words dictionary
#                         file_name = json_file.split('.')[0] # Remove the file extension
#                         if file_name not in found_words:
#                             found_words[file_name] = []
#                             found_words[file_name].append(found_word_data)

#         return found_words

def nearest_word_check(values,ocr_box):
    for value in values:
        for key,box in value.items():
            x1, y1, x2, y2 = box[0], box[1], ocr_box[0], ocr_box[1]
            distance = math.sqrt((x2 - x1)**2 + (y2 - y1)**2)
            if distance<=100:
                return key
            
    return ''
    
    
def find_nearest_words_in_ocr(values,dicts,ocr_data):
    return_dict={}
    for key,value in dicts.items():
        for items in value:
            for word in ocr_data:
                ocr_word = word["word"]
                similarity = fuzz.ratio(ocr_word.lower(), items.lower())
                
                if similarity>=90:
                    print(ocr_word)
                    box= [word["left"], word["top"], word["right"], word["bottom"]]
                    found_word=nearest_word_check(values,box)
                    print(f"similarity for {items} and {ocr_word} is {similarity}")
                    if found_word:
                        return_dict[key]=found_word
                    else:
                        print(F"no word found near this ocr_word {word}")
                        pass
                else:
                    pass
    return return_dict

def get_json_files_for_file_type(file_type,tenant_id):
    try:
        file_type = file_type.lower().replace(" ", "_")
        config_file = f'/var/www/prediction_api/app/data/{tenant_id}/all_config.json'
        
        # Load the configuration JSON file
        with open(config_file, 'r', encoding='utf-8') as file:
            config_data = json.load(file)
        
        json_files = []

        # Iterate through the configuration data to find the matching file_type
        for config_entry in config_data:
            if file_type in config_entry:
                file_path=f'/var/www/prediction_api/app/data/{tenant_id}/'+config_entry[file_type]
                # json_files.append(config_entry[file_type])
                json_files.append(file_path)

        return json_files
    except Exception as e:
        print(f"Error ocuures in fn get_json_files_for_file_type error is {e}")
        return []

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

def remove_name_country_from_address(input_dict):
    try:
        # Initialize the modified dictionary as a copy of the input dictionary
        modified_dict = input_dict.copy()

        # Create a list of keys with 'address' in them
        address_keys = [key for key in input_dict if 'address' in key]

        # Create a set of words to be checked (i.e., 'name' and 'country')
        words_to_check = set()
        for key in input_dict:
            if 'name' in key or 'country' in key:
                words_to_check.update(input_dict[key].split())

        # Iterate through the keys with 'address'
        for addr_key in address_keys:
            address_text = modified_dict[addr_key]

            # Check if any of the words to be checked are present in the 'address' text
            for word in words_to_check:
                if word in address_text:
                    # Remove the word from the 'address' text
                    address_text = address_text.replace(word, '')

            # Update the modified dictionary with the cleaned 'address' text
            modified_dict[addr_key] = address_text.strip()  # Remove leading/trailing spaces

        return modified_dict

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return input_dict



def process_country_reg_num(input_item, key,tenant_id):
    # Define a regular expression pattern to match alphanumeric values within parentheses
    # Initialize variables to store extracted values
    registration_number = None
    country = None

    # Check if the key contains 'name'
    if 'name' in key:
        # Define a regular expression pattern to match alphanumeric values
        words = re.split(r'\s+', input_item)


        words = [word for word in words if word]
        input_item=words[-1]
        if not input_item.isalpha():
        
            alphanumeric_pattern = r'\b([A-Za-z0-9-]+)\b'
            # Find the last alphanumeric word in the input_item
            matches = re.findall(alphanumeric_pattern, input_item)
            if matches:
                registration_number = matches[-1]

            if 'name' in key:
                return {
                        'registration_number': registration_number
                    }
        else:
            if 'name' in key:
                return {
                        'registration_number': ''
                    }
            
    # Check if the key contains 'address'
    if 'address' in key:
        # Load the list of countries from a JSON file
        with open(f'/var/www/prediction_api/app/data/{tenant_id}/countries_corpus.json', 'r') as countries_file:
            countries = json.load(countries_file)

        # Split the address into words and look for a country match
        words = input_item.split()
        for word in words:
            clean_word = ''.join(filter(str.isalnum, word.lower()))
            if clean_word in countries:
                country = word
                break

        return {        
            'country': country
            }
    return None

def get_country_reg_no_old(input_dict):
    output_dict={}
    for key, value in input_dict.items():
        
        if 'name' in key:
            print(key)
            processed_values = process_country_reg_num(value, key)
            stripped_key = key.rsplit('_', 1)[0]  # Get the stripped key until the last underscore
            output_dict[key] = value  # Keep the original item
            output_dict[stripped_key + '_registration_number'] = processed_values['registration_number']
        if 'address' in key:
            processed_values = process_country_reg_num(value, key)
            stripped_key = key.rsplit('_', 1)[0]  
            output_dict[key] = value 
            output_dict[stripped_key + '_country'] = processed_values['country']
    return output_dict

def get_country_reg_no(input_dict,pos_config_file,tenant_id):
    output_dict = {}

    has_name_or_address = False  # A flag to check if any 'name' or 'address' keys are present

    for key, value in input_dict.items():
        if 'name' in key:
            has_name_or_address = True
            processed_values = process_country_reg_num(value, key,tenant_id)
            stripped_key = key.rsplit('_', 1)[0]  # Get the stripped key until the last underscore
            output_dict[key] = value  # Keep the original item
            output_dict[stripped_key + '_registration_number'] = processed_values['registration_number']
        elif 'country' not in key:
            if 'address' in key:
                has_name_or_address = True
                processed_values = process_country_reg_num(value, key,tenant_id)
                stripped_key = key.rsplit('_', 1)[0]
                output_dict[key] = value
                
                output_dict[stripped_key + '_country'] = processed_values['country']
        else:
            output_dict[key] = value  

    if not has_name_or_address:
        return input_dict 
  
 
    for key in input_dict.keys():
        if 'name' not in key and 'address' and 'country' not in key:
            output_dict[key] = input_dict[key]
            
    print(f"In get country and reg numbers output_dict before is {output_dict}")
    updated_dict=update_output_keys(output_dict,pos_config_file)
    print(f"In get country and reg numbers output_dict after is {updated_dict}")

    return updated_dict


def update_output_keys(output_dict,pos_config_file):
    updated_output_dict = {}
    with open(pos_config_file, 'r',encoding='utf-8') as json_file:
        json_data = json.load(json_file)
    
    for key, value in output_dict.items():
        if 'registration_number' in key or 'country' in key:
            print(key)
            # Search for the key in the JSON data
            if key in json_data:
                mapping = json_data[key]
                if isinstance(mapping, dict):
                    # Get the first value from the mapping and use it as the new key
                    new_key = next(iter(mapping.values()))
                    updated_output_dict[new_key] = value
                else:
                    updated_output_dict[key] = value
        else:
            updated_output_dict[key] = value
    
    return updated_output_dict



def get_layout_labels__(file_type,input_dict,ocr_data,tenant_id):
    try:
        file_type = file_type.lower().replace(" ", "_")
        print(f"in get_layout labels input_dict got is {input_dict}")
        labels_final={}
        json_files=get_json_files_for_file_type(file_type,tenant_id)
        print(f"json files fetched in get_layout_labels, executing find_nearest words")
        found_words = find_nearest_words_in_ocr(input_dict, ocr_data, json_files)
        print(f"in get_layout_labels..completed find_nearest words")
        if found_words:
            labels_final=map_neighbour_dict(found_words,input_dict)
        else:
            print("in get in_layout_labels..no nearest keys found executing calculate_relative_positions")    
            raw_labels=calculate_relative_positions(input_dict)
            print(f"output got from calculate_relative_positions is{raw_labels}")
            pos_config_file=f'/var/www/prediction_api/app/data/model_files/{file_type}_pos_config.json'
            print(f"In get_layout_labels fn going to execute create_combined_dict")
            raw_labels2=create_combined_dict(raw_labels,pos_config_file)
            print(f"In get_layout_labels fn going to get_country_reg_no fn")
            
            final_labels=get_country_reg_no(raw_labels2,pos_config_file)
            print(f"final labels after getting reg and countries are {final_labels}")


            
        return final_labels
    except Exception as e:
        print(f"error occured in get_invoice_labels fn error is {e}")
        return {}
    

def get_layout_labels(file_type,input_dict,ocr_data,tenant_id):
    try:
        file_type = file_type.lower().replace(" ", "_")
        labels_final={}
        temp_input={}
        for key,values in input_dict.items():
            config_file = f'/var/www/prediction_api/app/data/{tenant_id}/{key}.json'
            try:
                with open(config_file, 'r', encoding='utf-8') as file:
                    dicts = json.load(file)
                print(f"dicts is {dicts}")
                temp = find_nearest_words_in_ocr(values,dicts,ocr_data)
                labels_final.update(temp)
                print(f"updated labels after finding nearest words in ocr {labels_final}")

                for key_,value in temp.items():
                    filtered_list = [item for item in input_dict[key] if value not in item]
                    temp_input[key] = filtered_list
            except FileNotFoundError:
                print(f"JSON file not found for key: {key}. adding it to temp_input...")
                temp_input[key]=values
        if temp_input and labels_final:
            print(f"after finding nearesr words remining are {temp_input}")
            result={}
            for key,values in temp_input.items():
                result[key] = calculate_relative_positions(values,{})
            
            pos_config_file=f'/var/www/prediction_api/app/data/{tenant_id}/{file_type}_pos_config.json'
            print(f"In get_invoice_labels fn going to execute create_combined_dict")
            final_labels=create_combined_dict(result,pos_config_file)
            print(f"labels got for temp_input items : {final_labels}")
            print(f"In get_invoice_labels fn going to execute_name_country_from_address fn")
            # temp__=remove_name_country_from_address(final_labels)
            # labels_final.update(temp__)
            temp__=get_country_reg_no(final_labels,pos_config_file,tenant_id)
            for label,word in temp__.items():
                if label not in labels_final:
                    labels_final[label]=word
            # labels_final.update(final_labels)
            print(f"final labels got after get_country_reg_no {labels_final}")
        else:
            print(f"in else condition no near ocr words found input dict is {input_dict}")
            result={}
            for key,values in input_dict.items():
                result[key] = calculate_relative_positions(values,{})
            print(f"postions obtainted are {result}")
            pos_config_file=f'/var/www/prediction_api/app/data/{tenant_id}/{file_type}_pos_config.json'
            print(f"In get_invoice_labels fn going to execute create_combined_dict")
            final_labels=create_combined_dict(result,pos_config_file)
            print(f"labels for the positions are {final_labels}")
            print(f"In get_invoice_labels fn going to execute_name_country_from_address fn")

            # temp__=remove_name_country_from_address(final_labels)
            # labels_final.update(temp__)
            # print(f"final labels got after removing nameand country {labels_final}")
            temp__=get_country_reg_no(final_labels,pos_config_file,tenant_id)
            labels_final.update(temp__)
            # labels_final.update(final_labels)
            print(f"final labels after removing country and reg are {labels_final}")
        


        return labels_final
    except Exception as e:
        print(f"error occured in get_invoice_labels fn error is {e}")
        return {}
