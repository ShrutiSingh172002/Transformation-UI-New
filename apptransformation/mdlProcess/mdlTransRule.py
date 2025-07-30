import re

# REPLACE_FIELD_WITH_VALUE
def replace_field_with_value(df, field_name, new_value):
    dicResult = {}
    try:
        dicResult['iserror'] = False

        # Replace all values in the column with a specific value
        df[field_name] = new_value
        
    except Exception as e:
        dicResult['iserror'] = True
        dicResult['error'] = "Unhandled Error While Processing REPLACE_FIELD_WITH_VALUE Rule."
        dicResult['error_details'] = dicResult['error'] + f" | Field: '{field_name}' | Error: {str(e)}"
    
    return dicResult

# REPLACE_VALUE_CASE_INSENSITIVE
def replace_value_case_insensitive(df, field_name, old_value, new_value):
    dicResult = {}
    try:
        dicResult['iserror'] = False

        # Convert everything to string first (safe replacement)
        df[field_name] = df[field_name].astype(str).apply(
            lambda x: new_value if x.strip().lower() == str(old_value).strip().lower() else x
        )

    except Exception as e:
        dicResult['iserror'] = True
        dicResult['error'] = "Unhandled Error While Processing REPLACE_VALUE_CASE_INSENSITIVE Rule."
        dicResult['error_details'] = dicResult['error'] + f" | Field: '{field_name}' | Error: {str(e)}"
    
    return dicResult

# REPLACE_VALUE
def replace_value(df, field_name, old_value, new_value):
    dicResult = {}
    try:
        dicResult['iserror'] = False

        # Replace old_value with new_value in the given column (vectorized)
        df[field_name] = df[field_name].replace(old_value, new_value)

    except Exception as e:
        dicResult['iserror'] = True
        dicResult['error'] = "Unhandled Error While Processing REPLACE_VALUE Rule."
        dicResult['error_details'] = dicResult['error'] + f" | Field: '{field_name}' | Error: {str(e)}"
    
    return dicResult

# ADDPREFIX
def add_prefix_suffix(df, field_name, affix, position='LEFT'):
        
    # Initialize variables
    dicResult = {} #Function Result Dictionary
    try:
        dicResult['iserror'] = False

        if position == 'LEFT':
            df[field_name] = affix + df[field_name].astype(str)
        else:
            df[field_name] = df[field_name].astype(str) + affix

    except (ValueError, TypeError) as e:
        dicResult['iserror'] = True
        dicResult['error'] = "Unhandled Error While Processsing ADDPREFIX Rule."
        dicResult['error_details'] = dicResult['error']  + " | " + str(e)
    return dicResult

# STRIP_WHITESPACE
def strip_whitespace(df, field_name):
    dicResult = {}
    try:
        dicResult['iserror'] = False

        # Strip leading and trailing whitespace (vectorized)
        df[field_name] = df[field_name].astype(str).str.strip()

    except Exception as e:
        dicResult['iserror'] = True
        dicResult['error'] = "Unhandled Error While Processing STRIP_WHITESPACE Rule."
        dicResult['error_details'] = dicResult['error'] + f" | Field: '{field_name}' | Error: {str(e)}"
    
    return dicResult

# ZEROFILL
def zero_pad_field(df, field_name, max_length):
    # Initialize variables
    dicResult = {} #Function Result Dictionary
    try:
        dicResult['iserror'] = False
        # Convert max_length to integer if it's a string
        if isinstance(max_length, str):
            max_length = int(max_length.strip())

        # Verify max_length is a positive integer
        if not isinstance(max_length, int) or max_length <= 0:
            dicResult['iserror'] = True
            dicResult['error'] = "Unhandled Error While Processsing ZEROFILL Rule."
            dicResult['error_details'] = dicResult['error']  + " | " + "Warning: Invalid max_length '{max_length}' for field '{field_name}' - {str(e)}"
            return dicResult
        
        # Perform zero padding
        df[field_name] = df[field_name].astype(str).str.zfill(max_length)
        
    except (ValueError, TypeError) as e:
        dicResult['iserror'] = True
        dicResult['error'] = "Unhandled Error While Processsing ZEROFILL Rule."
        dicResult['error_details'] = dicResult['error']  + " | " + f"Warning: Invalid max_length '{max_length}' for field '{field_name}' - {str(e)}"
    return dicResult

# SPECIAL_CHAR_REMOVAL
def remove_special_characters(df, field_name, allowed_chars=""):
    dicResult = {}  # Function result dictionary
    try:
        dicResult['iserror'] = False

        # Validate input types
        if not isinstance(allowed_chars, str):
            raise TypeError("allowed_chars must be a string.")

        # Build regex pattern dynamically
        # Keep alphanumeric characters and any allowed ones
        safe_chars = re.escape(allowed_chars)
        pattern = f"[^\w{safe_chars}]+"  # \w includes a-zA-Z0-9_

        # Apply regex replacement to remove unwanted characters
         # Vectorized operation
        df[field_name] = df[field_name].astype(str).str.replace(pattern, '', regex=True)

    except Exception as e:
        dicResult['iserror'] = True
        dicResult['error'] = "Unhandled Error While Processing SPECIAL_CHAR_REMOVAL Rule."
        dicResult['error_details'] = dicResult['error'] + f" | Field: '{field_name}' | Error: {str(e)}"

    return dicResult