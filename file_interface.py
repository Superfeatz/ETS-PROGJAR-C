import os
import json
import base64
from glob import glob


class FileInterface:
    def __init__(self):
        os.chdir('files/')

    def list(self, params=[]):
        try:
            filelist = glob('*.*')
            return dict(status='OK', data=filelist)
        except Exception as e:
            return dict(status='ERROR', data=str(e))

    def get(self, params=[]):
        try:
            filename = params[0]
            if (filename == ''):
                return dict(status='ERROR', data='Nama file tidak boleh kosong')
            fp = open(f"{filename}", 'rb')
            isifile = base64.b64encode(fp.read()).decode()
            fp.close()
            return dict(status='OK', data_namafile=filename, data_file=isifile)
        except Exception as e:
            return dict(status='ERROR', data=str(e))
    
    def upload(self, params=[]):
        try:
            if len(params) < 2:
                return dict(status='ERROR', data='Parameter kurang')
            filename = params[0]
            isifile = base64.b64decode(params[1])
            with open(filename, 'wb') as fp:
                fp.write(isifile)
            return dict(status='OK', data='File berhasil diupload')
        except Exception as e:
            return dict(status='ERROR', data=str(e))
    
    def delete(self, params=[]):
        try:
            if len(params) < 1:
                return dict(status='ERROR', data='Parameter kurang')
            filename = params[0]
            if os.path.exists(filename):
                os.remove(filename)
                return dict(status='OK', data='File berhasil dihapus')
            else:
                return dict(status='ERROR', data='File tidak ditemukan')
        except Exception as e:
            return dict(status='ERROR', data=str(e))


if __name__=='__main__':
    f = FileInterface()