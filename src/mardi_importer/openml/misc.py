from datetime import datetime

def convert_time_to_iso(time_string):
    try:
        # Attempt to parse the input time string
        dt = datetime.strptime(time_string, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        try:
            dt = datetime.strptime(time_string, "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            try:
                dt = datetime.strptime(time_string, "%Y-%m-%d")
            except ValueError:
                # Handle other formats as needed
                print(time_string)
                print(a)
                return None
    
    # Convert to ISO format
    iso_format = dt.isoformat() + "Z"
    return iso_format