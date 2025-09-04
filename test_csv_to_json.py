import csv
import ftplib
import json
import time
from datetime import datetime
filename_with_time_stamp=""
from ftplib import FTP_TLS

def createFtpConnection():
    # Connect FTP Server
    ftp_server = ftplib.FTP("data.capillarydata.com", "capillary", "captech123")
    # force UTF-8 encoding
    ftp_server.encoding = "utf-8"
    return ftp_server

def createFtpConnection1():
    ftp_server = FTP_TLS("data.capillarydata.com", "capillary", "captech123")
    ftp_server.encoding = "utf-8"
    ftp_server.prot_p()  # Secure data connection
    print("Secure connection established!")
    return ftp_server

def listingFiles(remote_dir_path):
    ftp_server = createFtpConnection()
    # Get a list of files in the remote directory
    file_list = ftp_server.nlst(remote_dir_path)
    ftp_server.quit()
    # Logging the list of files inside the FTP server
    return file_list

def csvFileWithDataTime(filename):
    # Added timestamp to csv file for ignoring conflict
    timestamp = datetime.now().strftime('%Y-%m-%d--%H:%M:%S')
    # added time stamp to csv file
    filenameWithTimeStamp = filename + '-{}.csv'.format(timestamp)
    return filenameWithTimeStamp

def uploadToFtpServer(remote_file_path, local_file_path):
    max_retries = 3
    retries = 0
    success = False

    while retries < max_retries and not success:
        ftp_server = None
        try:
            ftp_server = createFtpConnection()
            with open(local_file_path, 'rb') as local_file:
                ftp_server.storbinary('STOR ' + remote_file_path, local_file)
            file_size = ftp_server.size(remote_file_path)
            if file_size == 0:
                ftp_server.delete(remote_file_path)
                raise Exception("Uploaded file is zero bytes, deleting and retrying...")
            success = True

        except Exception as e:
            retries += 1
            if retries < max_retries:
                print("sleep")

        finally:
            # Close the Connection
            if ftp_server:
                ftp_server.quit()

    if not success:
        error_message = f"Failed to upload the file after {max_retries} attempts"
        raise AssertionError(error_message)



def downloadFromFtpServer(remote_file_path, local_file_path):
    # Connect FTP Server
    ftp_server =createFtpConnection()

    # downloaded file from ftp
    try:
        # Command for Downloading the file "RETR filename"
        with open(local_file_path, 'wb') as local_file:
            ftp_server.retrbinary('RETR ' + remote_file_path, local_file.write)

    except:
        print("exception")

    # Display the content of downloaded file
    file = open(local_file_path, "r")

    # Close the Connection
    ftp_server.quit()

def deleteFileFromFtpServer(remote_file_path):
    ftp_server = createFtpConnection()
    ftp_server.delete(remote_file_path)

def downloadFromFtpServer1(remote_file_path, local_file_path):
    # Connect FTP Server
    ftp_server = createFtpConnection()

    # Download file from FTP
    try:
        # Command for downloading the file "RETR filename"
        with open(local_file_path, 'wb') as local_file:
            ftp_server.retrbinary('RETR ' + remote_file_path, local_file.write)
        print("Download successful")
    except Exception as e:
        print(f"Exception during download: {e}")

    # Read and return the content of the downloaded file
    try:
        with open(local_file_path, "r") as file:
            content = file.read()
        return content
    except Exception as e:
        print(f"Exception reading the file: {e}")
        return None
    finally:
        # Close the Connection
        ftp_server.quit()



def download_file( remote_path, local_path):
    with open(local_path, 'wb') as file:
        ftp=createFtpConnection()
        ftp.retrbinary(f'RETR {remote_path}', file.write)
    with open(local_path, 'r') as file:
        return json.load(file)


def normalize(data):
    """ Normalize the data by converting to lowercase and stripping spaces. """
    return [
        sorted(
            [
                {k: v.lower().strip() if isinstance(v, str) else v for k, v in item.items()}
                for item in sublist
            ],
            key=lambda x: (x['name'], x['age'], x['state'], x['country'])
        )
        for sublist in data
    ]


def convert_to_frozenset(data):
    return [frozenset(frozenset(item.items()) for item in sublist) for sublist in data]

def compare_data(fetched, processed):
    fetched_set =     set(fetched)
    processed_set=set(processed)

    if fetched_set == processed_set:
        return "The data lists are equivalent."
    else:
        return "The data lists are not equivalent."

def flatten_and_sort_list_of_dicts(list_of_lists):
    # Flatten the list of lists
    flat_list = [item for sublist in list_of_lists for item in sublist]
    # Sort the flattened list of dictionaries
    sorted_flat_list = sorted(flat_list, key=lambda x: sorted(x.items()))
    return sorted_flat_list

def retrieve():
    processed_data = [
        [{"name": "user6", "age": "20", "state": "state3", "country": "country3"}],
        [
            {"name": "user1", "age": "23", "state": "state1", "country": "country1"},
            {"name": "user2", "age": "23", "state": "state1", "country": "country1"},
            {"name": "user3", "age": "22", "state": "state1", "country": "country1"}
        ],
        [
            {"name": "user4", "age": "23", "state": "state2", "country": "country2"},
            {"name": "user5", "age": "22", "state": "state2", "country": "country2"}
        ]
    ]
    ftp_remote_dir = "/Capillary testing/automation/nightly_cc/1115/csvToJsonConverter/remote"
    time.sleep(5)
    files_in_remote_dir = listingFiles(ftp_remote_dir)

    fetched_data = []
    for file in files_in_remote_dir:

        if file.startswith(filename_with_time_stamp):
            ftp_process_path = ftp_remote_dir + "/{}".format(file)
            local_path = "/tmp/{}".format(file)
            file_data = download_file(ftp_process_path, local_path)
            fetched_data.append(file_data)

    flatten_fetch = flatten_and_sort_list_of_dicts(fetched_data)
    flatten_processed = flatten_and_sort_list_of_dicts(processed_data)
    print(f"Files data in directory: {flatten_fetch}")
    print(f"assertion data : {flatten_processed}")
    for file in files_in_remote_dir:
        ftp_remote_path = ftp_remote_dir + "/{}".format(file)
        if file.startswith(filename_with_time_stamp):
            print(f"file name with timestamp: {filename_with_time_stamp}")
            print(f"Deleting file: {ftp_remote_path}")
            deleteFileFromFtpServer(ftp_remote_path)

    if flatten_fetch == flatten_processed:
        print("data in remote files are as expected")




# retrieve()

def test_csv2json_sanity():
    filename = "csvfile"
    filename_with_time_stamp = csvFileWithDataTime(filename)
    local_path = "/tmp/" + filename_with_time_stamp

    csv_headers = ["name", "age", "state", "country"]
    csv_rows = [
        ["user1", "23", "state1", "country1"],
        ["user2", "23", "state1", "country1"],
        ["user3", "22", "state1", "country1"],
        ["user4", "23", "state2", "country2"],
        ["user5", "22", "state2", "country2"],
        ["user6", "20", "state3", "country3"]
    ]

    with open(local_path, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(csv_headers)
        writer.writerows(csv_rows)

    ftp_source_dir = "/Capillary testing/automation/nightly_cc/1115/csvToJsonConverter/source"
    ftp_source_path = ftp_source_dir + "/{}".format(filename_with_time_stamp)
    uploadToFtpServer(ftp_source_path, local_path)

    # time.sleep(5)
    processed_data = [
        [{"name": "user6", "age": "20", "state": "state3", "country": "country3"}],
        [
            {"name": "user1", "age": "23", "state": "state1", "country": "country1"},
            {"name": "user2", "age": "23", "state": "state1", "country": "country1"},
            {"name": "user3", "age": "22", "state": "state1", "country": "country1"}
        ],
        [
            {"name": "user4", "age": "23", "state": "state2", "country": "country2"},
            {"name": "user5", "age": "22", "state": "state2", "country": "country2"}
        ]
    ]
    ftp_remote_dir = "/Capillary testing/automation/nightly_cc/1115/csvToJsonConverter/remote"
    time.sleep(5)
    files_in_remote_dir = listingFiles(ftp_remote_dir)

    fetched_data = []
    for file in files_in_remote_dir:

        if file.startswith(filename_with_time_stamp):
            ftp_process_path = ftp_remote_dir + "/{}".format(file)
            local_path = "/tmp/{}".format(file)
            file_data = download_file(ftp_process_path, local_path)
            fetched_data.append(file_data)

    flatten_fetch = flatten_and_sort_list_of_dicts(fetched_data)
    flatten_processed = flatten_and_sort_list_of_dicts(processed_data)
    print(f"Files data in directory: {flatten_fetch}")
    print(f"assertion data : {flatten_processed}")
    for file in files_in_remote_dir:
        ftp_remote_path = ftp_remote_dir + "/{}".format(file)
        if file.startswith(filename_with_time_stamp):
            print(f"file name with timestamp: {filename_with_time_stamp}")
            print(f"Deleting file: {ftp_remote_path}")
            deleteFileFromFtpServer(ftp_remote_path)

    if flatten_fetch == flatten_processed:
        print("data in remote files are as expected")


# Call the test function
test_csv2json_sanity()