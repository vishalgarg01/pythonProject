import ftplib
import time
import traceback
import socket
import json


class FtpHelper(): ## Rename this to FtpHelper update across

    @staticmethod
    def createFtpConnection():
        # Connect FTP Server
        ftp_server = ftplib.FTP("data.capillarydata.com", "capillary", "captech123")

        #ftp_server.af = socket.AF_INET6
        # force UTF-8 encoding
        ftp_server.encoding = "utf-8"
        return ftp_server

    @staticmethod
    def createFtpDir(path):
        # Connect FTP Server
        ftp_server = FtpHelper.createFtpConnection()

        # Split the path into individual directory names
        pathDirectory = path.split("/")
        # Create each directory one by one, starting from the root directory
        for pathDir in pathDirectory:
            try:
                ftp_server.cwd(pathDir)
            except ftplib.error_perm:
                ftp_server.mkd(pathDir)
                ftp_server.cwd(pathDir)

        # Close the Connection
        FtpHelper.closeFtpConnection(ftp_server)

    @staticmethod
    def closeFtpConnection(ftpServer):
        # Close the Connection
        ftpServer.quit()

    @staticmethod
    def listingFiles(remote_dir_path):
        ftp_server = FtpHelper.createFtpConnection()
        # Get a list of files in the remote directory
        file_list = ftp_server.nlst(remote_dir_path)
        FtpHelper.closeFtpConnection(ftp_server)
        # Logging the list of files inside the FTP server
        return file_list

    @staticmethod
    def uploadToFtpServer(remote_file_path, local_file_path):
        max_retries = 3
        retries = 0
        success = False

        while retries < max_retries and not success:
            try:
                ftp_server = FtpHelper.createFtpConnection()
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
                    time.sleep(5)  # Wait for 5 seconds before retrying

            finally:
                # Close the Connection
                FtpHelper.closeFtpConnection(ftp_server)

        if not success:
            error_message = f"Failed to upload the file after 3 attempts"
            raise AssertionError( error_message)

    @staticmethod
    def deleteFileFromFtpServer(remote_file_path):
        ftp_server = FtpHelper.createFtpConnection()
        ftp_server.delete(remote_file_path)

    @staticmethod
    def downloadFromFtpServer(remote_file_path, local_file_path):
        # Connect FTP Server
        ftp_server = FtpHelper.createFtpConnection()

        # downloaded file from ftp
        try:
            # Command for Downloading the file "RETR filename"
            with open(local_file_path, 'wb') as local_file:
                ftp_server.retrbinary('RETR ' + remote_file_path, local_file.write)

        except:
            print(traceback.format_exc())

        # Display the content of downloaded file
        file = open(local_file_path, "r")

        # Close the Connection
        FtpHelper.closeFtpConnection(ftp_server)

    @staticmethod
    def download_file(remote_path, local_path):
        ftp = None
        try:
            with open(local_path, 'wb') as file:
                ftp = FtpHelper.createFtpConnection()
                ftp.retrbinary(f'RETR {remote_path}', file.write)
            with open(local_path, 'r') as file:
                return json.load(file)
        except:
            print(f"exception while downloading file : {traceback.format_exc()}")
        finally:
            if ftp:
                ftp.quit()
