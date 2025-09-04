from ftpHelper import FtpHelper


class CsvToJsonHelper():
    @staticmethod
    def csvToJsonHelper(validation_data):
        process_file_name = validation_data.get("fileName", "")
        print(f"Start csvToJson helper for filename: {process_file_name}")
        assertion_data = validation_data.get("assertionData", "")

        try:
            ftp_remote_dir = "/Capillary testing/automation/{}/{}/{}/remote".format(
                "nightly_cc", "1115", "csvToJsonConverter"
            )

            files_in_remote_dir = FtpHelper.listingFiles(ftp_remote_dir)
            filtered_files_in_remote_dir = [file for file in files_in_remote_dir if file.startswith(process_file_name)]

            if len(filtered_files_in_remote_dir) == 3 :
                print("Successfully got 3 success files")
        except Exception as e:
            print(f"Error checking remote directory: {e} for file {process_file_name}")
            return

        fetched_data = []
        try:
            for file in filtered_files_in_remote_dir:
                if file.startswith(process_file_name):
                    print("Success file name is proper")
                ftp_process_path = f"{ftp_remote_dir}/{file}"
                local_path = f"/tmp/{file}"
                print(f"Downloading file: {ftp_process_path} to {local_path}")
                file_data = FtpHelper.download_file(ftp_process_path, local_path)
                fetched_data.append(file_data)
        except Exception as e:
            print(f"Error fetching data: {e} for file {process_file_name}")
            return

        try:
            print(f"Files data fetched from directory: {fetched_data} for file {process_file_name}")

            flatten_fetch = CsvToJsonHelper.flatten_and_sort_list_of_dicts(fetched_data)
            flatten_processed = CsvToJsonHelper.flatten_and_sort_list_of_dicts(assertion_data)

            print(f"Flattened files data in directory: {flatten_fetch} for file {process_file_name}")
            print(f"Assertion data: {flatten_processed} for file {process_file_name}")

            for file in filtered_files_in_remote_dir:
                ftp_remote_path = f"{ftp_remote_dir}/{file}"
                print(f"Deleting file: {ftp_remote_path}")
                FtpHelper.deleteFileFromFtpServer(ftp_remote_path)

            print(f"Validation for file {process_file_name} is {flatten_fetch == flatten_processed}")
        except Exception as e:
            print(f"Error in validation or file deletion: {e} for file {process_file_name}")
            return

    @staticmethod
    def flatten_and_sort_list_of_dicts(list_of_lists):
        try:
            # Flatten the list of lists
            flat_list = [item for sublist in list_of_lists for item in sublist]
            # Sort the flattened list of dictionaries
            sorted_flat_list = sorted(flat_list, key=lambda x: sorted(x.items()))
            return sorted_flat_list
        except Exception as e:
            print(f"Error flattening and sorting list of dicts: {e}")
            return []
