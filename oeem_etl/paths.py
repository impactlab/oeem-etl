import os

def gather_paths(root):
    paths = []
    for subdir, dirs, files in os.walk(root):
        for filename in files:
            full_path = os.path.join(subdir, filename)
            if full_path.endswith("csv"):
                paths.append(full_path)
    return paths

def mirror_path(path, from_path, to_path):
    """Replaces the prefix `from_path` on `path` with `to_path`

    e.g.

    mirror_path("/home/user/data/client/raw/consumptions/000.csv", 
                "/home/user/data/client/raw",
                "/home/user/data/client/uploaded")
    
    returns

    "/home/user/data/client/uploaded/consumptions/000.csv"    

    """

    # Returns path with any directories nested under `raw` and the filename
    nested_path = os.path.relpath(path, from_path)    
    output_path = os.path.join(to_path, nested_path)
    return output_path