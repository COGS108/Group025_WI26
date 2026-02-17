import os
import requests
from tqdm import tqdm

def get_raw(file_list, destination_directory):
    """
    Downloads a list of files to a specified destination directory with progress bars.

    Args:
        file_list (list): A list of dictionaries, where each dictionary
                          contains 'url' (the URL of the file) and 'filename'
                          (the desired local filename).
        destination_directory (str): The path to the directory where files
                                     should be saved.
    """
    if not os.path.exists(destination_directory):
        print(f"Error directory {destination_directory} does not exist")
        return

    for file_info in tqdm(file_list, desc="Overall Download Progress"):
        url = file_info['url']
        filename = file_info['filename']
        local_filepath = os.path.join(destination_directory, filename)

        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()  # Raise an exception for bad status codes

            total_size = int(response.headers.get('content-length', 0))
            block_size = 1024  # 1 KB

            with open(local_filepath, 'wb') as f:
                with tqdm(total=total_size, unit='B', unit_scale=True,
                          desc=f"Downloading {filename}", leave=False) as pbar:
                    for chunk in response.iter_content(chunk_size=block_size):
                        if chunk:  # filter out keep-alive new chunks
                            f.write(chunk)
                            pbar.update(len(chunk))
            print(f"Successfully downloaded: {filename}")
        except requests.exceptions.RequestException as e:
            print(f"Error downloading {filename}: {e}")
        except Exception as e:
            print(f"An unexpected error occurred for {filename}: {e}")

if __name__ == "__main__":
    import os
    
    destination = "data/00-raw"
    
    # Make sure folder exists
    os.makedirs(destination, exist_ok=True)

    nhanes_files = [
        {"url": "https://wwwn.cdc.gov/Nchs/Nhanes/2017-2018/DEMO_J.XPT", "filename": "DEMO_J.XPT"},
        {"url": "https://wwwn.cdc.gov/Nchs/Nhanes/2017-2018/GHB_J.XPT", "filename": "GHB_J.XPT"},
        {"url": "https://wwwn.cdc.gov/Nchs/Nhanes/2017-2018/GLU_J.XPT", "filename": "GLU_J.XPT"},
        {"url": "https://wwwn.cdc.gov/Nchs/Nhanes/2017-2018/HIQ_J.XPT", "filename": "HIQ_J.XPT"},
        {"url": "https://wwwn.cdc.gov/Nchs/Nhanes/2017-2018/SMQ_J.XPT", "filename": "SMQ_J.XPT"},
        {"url": "https://wwwn.cdc.gov/Nchs/Nhanes/2017-2018/ALQ_J.XPT", "filename": "ALQ_J.XPT"},
        {"url": "https://wwwn.cdc.gov/Nchs/Nhanes/2017-2018/PAQ_J.XPT", "filename": "PAQ_J.XPT"},
    ]

    get_raw(nhanes_files, destination)

demo = pd.read_sas(os.path.join(destination, "DEMO_J.XPT"))
    print(demo.head())
