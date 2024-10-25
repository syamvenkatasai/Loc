from ace_logger import Logging

logging = Logging()

def get_parents_list(df):
    return list(df[df['parent_id'].isnull()]['unique_key'])

def get_children(parent, all_files_df, df, parent_id_mapping):
    try:
        try:
            id_ = parent_id_mapping[parent]
        except:
            id_ = list(df[df['unique_key'] == parent]['id'])[0]
        values = list(df[df['parent_id'] == id_]['unique_key'])
        if not values:
            return list(all_files_df[all_files_df['folder_id'] == id_]['file_name'])
        return values
    except:
        return 'END'
       
def get_all_heirarchies(parent, parents_list, all_files_df, df, folder_mapping, parent_id_mapping):
    if parent not in parents_list:
        return parent
       
    children_tree_list = {}
    children = get_children(parent, all_files_df, df, parent_id_mapping)
    
    if type(children) == list:        
        if children:
            if children[0] not in parents_list:
                return children
        else:
            return children
            
    for child in children:
        if child == get_all_heirarchies(child, parents_list, all_files_df, df, folder_mapping, parent_id_mapping):
            children_tree_list.update({folder_mapping.get(child, child): child})
        else:
            children_tree_list.update({folder_mapping.get(child, child): get_all_heirarchies(child, parents_list, all_files_df, df, folder_mapping, parent_id_mapping)})    
    
    return children_tree_list

def clean_dict(d):
    if not isinstance(d, (dict, list)):
        return d
    if isinstance(d, list):
        return [v for v in (clean_dict(v) for v in d) if v]
    return {k: v for k, v in ((k, clean_dict(v)) for k, v in d.items()) if v}

def make_tree(df, all_files_df):
    heirarchy = {}
    parents = get_parents_list(df)
    parents_list = list(df['unique_key'])
    folder_mapping = dict(zip(list(df['unique_key']), list(df['folder_name'])))
    unique_folder_ids = list(all_files_df['folder_id'].unique())
    unique_folder_key = df[df['id'].isin(unique_folder_ids)]
    parent_id_mapping = dict(zip(list(unique_folder_key['unique_key']), list(unique_folder_key['id'])))
    for parent in parents:
        heirarchy.update({folder_mapping.get(parent, parent): get_all_heirarchies(parent, parents_list, all_files_df, df, folder_mapping, parent_id_mapping)})
    return clean_dict(heirarchy)