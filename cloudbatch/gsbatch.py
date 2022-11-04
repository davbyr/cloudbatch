import subprocess
import numpy as np
import os
import os.path as path
import glob
from .cloudbatch import CloudBatch

class GSBatch(CloudBatch):
    
    '''
    For batching objects in a Google Storage bucket. Will using gsutil to download and
    upload files in parallel.
    
    INPUTS
        file_list (list)  :: List of file names or complete file paths.
        file_dir (str)    :: Directory to append to front of all files in file_list.
                             [ Default = None ]
        get_dir (str)     :: Directory of where to download temporary batch files if
                             source = 'remote'. Required to use .get_batch()
        put_dir (str)     :: Directory of where to upload files if source = 'local'.
                             Required to use .put_batch()
        source (str)      :: Either 'remote' or 'local. Signifies whether the data 
                             files to be moved will be downloaded or uploaded.
        batch_size (int)  :: Number of files in a batch.
        
    METHODS
    '''
    
    def __init__(self, 
                 files = None, 
                 file_dir = None,
                 get_dir = None,
                 put_dir = None,
                 source = 'remote',
                 batch_size=10
                ):
            
        # Add directory to file names if wanted
        if file_dir is not None:
            files = [path.join(file_dir, fn) for fn in files]
        
        # Initialise batches
        self.current_file = 0
        self.current_batch = 0
        
        n_files = len(files)
        
        n_batches = np.ceil( n_files / batch_size ).astype(int)
        last_batch_size = n_files % batch_size
        
        self.n_batches = n_batches
        self.n_files = n_files
        self.files = files
        self.last_batch_size = last_batch_size
        self.batch_size = batch_size
        self.is_last_batch = False
        self.source = source
        self.tmp_files = []
        self.get_dir = get_dir
        self.put_dir = put_dir
        
        self._update_batch() 
        
        return
    
    def get_batch(self):  
        
        # Create get command using gsutil and run from command line
        get_cmd = 'gsutil -m cp '
        for ff in self.files_batch:
            get_cmd = get_cmd + f' {ff}'
            
        get_cmd += f' {self.get_dir}'
        subprocess.run(get_cmd, shell=True,
                       stdout=subprocess.DEVNULL,
                       stderr=subprocess.STDOUT,)
        
        # Save list of current temporary files
        got_files = []
        for ff in self.files_batch:
            got_files.append(path.join(self.get_dir, path.basename(ff)))
            
        # Check if successful
        for fn in got_files:
            if not path.isfile(fn):
                self.delete_tmp_files(got_files)
                raise Exception("Failed to download files.")
            
        self.tmp_files = self.tmp_files + got_files
        
    def put_batch(self):  
        put_cmd = f'gsutil -m cp '
        for ff in self.files_batch:
            put_cmd = put_cmd + f' {ff}'
        put_cmd += f' {self.put_dir}'
        subprocess.run(put_cmd, shell=True,
                       stdout=subprocess.DEVNULL,
                       stderr=subprocess.STDOUT,)
        return
        
    def check_files(self):
        
        checked = np.zeros(self.n_files)
        for ii in range(self.n_files):
            if self.source == 'remote':
                checked[ii] = self._gsstat(self.files[ii])
            elif self.source == 'local':
                checked[ii] = self._localstat(self.files[ii])
            print(ii, self.n_files, checked[ii])
                
        self.file_exists = checked
    
    def _gsls(self, path):
        
        cmd = f"gsutil ls {path}"
        output = subprocess.check_output(cmd, shell=True)
        output = str(output)[2:-1]
        
        output_split = output.split('\\n')[:-1]
        
        return output_split
    
    def _gsstat(self, path):
        cmd = f"gsutil stat {path}"
        try:
            output = subprocess.check_output(cmd, shell=True)
            if output[:2] == 'No':
                return False
            else:
                return True
        except:
            return False
            
        