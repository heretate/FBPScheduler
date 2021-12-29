from __future__ import annotations
import re


def flat_args(arguments: dict | list, delimiter="; ") -> str:
    """
    Flattens a dictionary or list into a single string to pass into command line
    with the format of: key="value" + delimiter or "value" + delimiter
        

    Parameters
    ----------
    arguments : dict | list
        A dictionary or list containing the arguments to be flattened.
    delimiter : str, optional
        Character(s) used to separate each character. The default is "; ".

    Returns
    -------
    new_string : str
        A continuous string of arguments

    """
    new_string = ""
    if isinstance(arguments, dict):
        for key, value in arguments.items():
            new_string = new_string + key + "=\"" + str(value) + "\"{delim}".format(delim=delimiter)

    if isinstance(arguments, list):
        for value in arguments:
            new_string = new_string + "\"" + str(value) + "\"{delim}".format(delim=delimiter)
    return new_string


def list_args(arguments: dict) -> list:
    """
    Converts a dictionary to a list of key-value pairs in the format of:
        key=value

    Parameters
    ----------
    arguments : dict
        A dictionary containing the arguments to be flattened.

    Returns
    -------
    new_list : str
       A list of key-value pairs.

    """
    new_list = []
    for key, value in arguments.items():
        new_list.append(key + "=" + str(value))
    
    return new_list
    

def fill_string(target_string: str, params: dict, partial_fill = False) -> str:
    """
    Fill in parameterized values within the target string with the params
    dictionary. Parameterized values should appear in the form of 
    #key# where key is in params

    Parameters
    ----------
    target_string : str
        A string representing the command/executable being called. Can be embedded
        with keywords delimited with # to be replaced by values in params.
        
    params : dict
        A dictionary of parameters.

    partial_fill : bool
        No longer outputs warning messages if a parameter could not be found.

    Returns
    -------
    target_string : str
        A filled in string.

    """
    if not isinstance(target_string, str):
        print("Target is not of instance str")
        return target_string

    pattern = "#(.*?)#"
    substring_match = re.search(pattern, target_string)
    try:
        while substring_match is not None:
            substring = substring_match.group(1)
            target_string = target_string.replace(substring_match.group(0), str(params[substring]))
            substring_match = re.search(pattern, target_string)
    except KeyError:
        if not partial_fill:
            print("Could not find parameter " + substring + " in passed parameters.")

    return target_string


def parse_arguments(arguments: dict | list, params: dict) -> dict:
    """
    Fill in parameterized values within the arguments dictionary with the params 
    dictionary. Parameterized values should appear in the form of 
    #key# where key is in params

    Parameters
    ----------
    arguments : dict | list
        A dictionary or list of arguments. Can be embedded with keywords delimited with
        # to be replaced by values in params.
    params : dict
        A dictionary of parameters.

    Returns
    -------
    args_copy : dict | list
        A dictionary or list of arguments with any parameterized arguments filled in
        from params.

    """
    
    args_copy = arguments.copy()
    pattern = "#(.*?)#"
    if isinstance(arguments, dict):
        for key, value in arguments.items():
            if not isinstance(value, str):
                continue
            substring_match = re.search(pattern, value)
            if substring_match is not None:
                substring = substring_match.group(1)
                args_copy[key] = params[substring]

    if isinstance(arguments, list):
        for i, value in enumerate(arguments):
            if not isinstance(value, str):
                continue
            substring_match = re.search(pattern, value)
            if substring_match is not None:
                substring = substring_match.group(1)
                args_copy[i] = params[substring]
            
    return args_copy

