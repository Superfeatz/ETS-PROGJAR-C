import json
import logging
import shlex

from file_interface import FileInterface

"""
* class FileProtocol bertugas untuk memproses 
data yang masuk, dan menerjemahkannya apakah sesuai dengan
protokol/aturan yang dibuat

* data yang masuk dari client adalah dalam bentuk bytes yang 
pada akhirnya akan diproses dalam bentuk string

* class FileProtocol akan memproses data yang masuk dalam bentuk
string
"""
class FileProtocol:
    def __init__(self):
        self.file = FileInterface()
        
    def proses_string(self, string_datamasuk=''):
        logging.warning(f"processing string of length: {len(string_datamasuk)}")
        try:
            # Use a more robust method for splitting commands
            # This helps handle large base64 encoded files without shlex issues
            if " " not in string_datamasuk:
                c_request = string_datamasuk.strip().lower()
                params = []
            else:
                parts = string_datamasuk.split(" ", 1)
                c_request = parts[0].strip().lower()
                
                if len(parts) > 1:
                    # For UPLOAD command, we need special handling due to large base64 content
                    if c_request == "upload":
                        # Split only on the first space after the filename
                        filename_and_content = parts[1].split(" ", 1)
                        params = filename_and_content
                    else:
                        # For other commands, use shlex for proper parameter parsing
                        try:
                            params = shlex.split(parts[1])
                        except Exception as e:
                            logging.warning(f"Error parsing parameters with shlex: {str(e)}")
                            params = parts[1].split()
                else:
                    params = []
            
            logging.warning(f"processing request: {c_request} with {len(params)} parameters")
            if hasattr(self.file, c_request):
                cl = getattr(self.file, c_request)(params)
                return json.dumps(cl)
            else:
                return json.dumps(dict(status='ERROR', data='Unknown command'))
        except Exception as e:
            logging.warning(f"Error processing request: {str(e)}")
            return json.dumps(dict(status='ERROR', data=f'Error processing request: {str(e)}'))


if __name__=='__main__':
    #contoh pemakaian
    fp = FileProtocol()