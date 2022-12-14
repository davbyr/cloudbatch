import subprocess
import numpy as np
import os
import os.path as path
import glob

class CloudBatch():
    
    def __init__(self, 
                 file_list = None, 
                 file_dir = None,
                 get_dir = None,
                 source = 'remote',
                 batch_size=10
                ):
        
        return
            
    def next_batch(self):
        if self.current_batch < self.n_batches - 1:
            self.current_batch = self.current_batch + 1
            self.current_file = self.current_file + self.batch_size
            self._update_batch()
        else:
            print('Final batch has been reached. Cannot increase index.')
        
    def prev_batch(self):
        if self.current_batch > 0:
            self.current_batch = self.current_batch - 1
            self.current_file = self.current_file - self.batch_size
            self._update_batch()
        else:
            print('This is the first batch. Cannot decrease index')
        
    def reset_batch(self):
        self.current_batch = 0
        self.current_file = 0
        self._update_batch()
        return
    
    def get_batch(self):
        error_msg = 'This child of CloudBatch has not implemented this method'
        raise NotImplemented(error_msg)
        
    def put_batch(self):  
        error_msg = 'This child of CloudBatch has not implemented this method'
        raise NotImplemented(error_msg)
        return
    
    def set_batch_size(self, batch_size):
        
        self.batch_size = batch_size
        n_batches = np.ceil( self.n_files / batch_size ).astype(int)
        last_batch_size = self.n_files % batch_size
        
        self.n_batches = n_batches
        self.last_batch_size = last_batch_size
        self._update_batch()
                        
    def is_final_batch(self):
        if self.current_batch == self.n_batches - 1:
            return True
        else:
            return False
        
    def delete_tmp_files(self, files_to_delete=None):
        if files_to_delete is None:
            files_to_delete = self.tmp_files.copy()
            self.tmp_files = []
            
        for ff in files_to_delete:
            try:
                os.remove(ff)
            except:
                pass
            
    def delete_batch_files(self):
        for ff in self.files_batch:
            os.remove(ff)
        self.files_batch = []
        
    def summary(self):
        out_str = ''
        print(f'   Number of batches:     {self.n_batches}')
        print(f'   Number of files:       {self.n_files}')
        print('')
        print(f'   Current batch number:  {self.current_batch+1}')
        
        
    def _update_batch(self):
        
        start_idx = self.current_file
        
        if self.current_batch < self.n_batches - 1:
            end_idx = self.current_file + self.batch_size
        else:
            end_idx = self.current_file + self.last_batch_size
        
        self.files_batch = self.files[start_idx:end_idx]
        
    def _localstat(self, path):
        return os.path.exists(path)
    
    def _ls(self, path):
        
        cmd = f"ls {path}"
        output = subprocess.check_output(cmd, shell=True)
        output = str(output)[2:-1]
        
        output_split = output.split('\\n')[:-1]
        
        return output_split
    
    def _expand_wildcards(self, list_of_paths, source):
        
        output_list = []
        n_str = len(list_of_paths)
        
        for ii in range(n_str):
            path = list_of_paths[ii]
            if '*' in path:
                if source == 'remote':
                    wc_files = self._gsls(path)
                elif source == 'local':
                    wc_files = glob.glob(path)
                if type(wc_files) is not list:
                    wc_files = [wc_files]
                output_list = output_list + wc_files
            else:
                output_list.append(path)
                
        return output_list
            
        