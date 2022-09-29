import subprocess
import numpy as np
import os
import os.path as path
import glob

class CloudBatch():
    
    def __init__(self, 
                 file_list = None, 
                 file_components = None,
                 file_dir = None,
                 get_dir = None,
                 put_dir = None,
                 source = 'remote',
                 batch_size=10
                ):

        # Define GS files
        if file_components is not None:
            n_components = len(file_components)
            files = self._make_files_from_components(file_components)
        elif file_list is not None:
            if type(file_list) is str:
                file_list = [file_list]
            files = file_list
        else:
            raise Exception("You need to provide file_list or file_components")
            
        # Add directory to file names if wanted
        if file_dir is not None:
            files = [path.join(file_dir, fn) for fn in files]
            
        # Check for wildcards
        files = self._expand_wildcards(files, source)
        
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
        
        # Create get command using gsutil and run from command line
        get_cmd = 'gsutil -m cp '
        for ff in self.files_batch:
            get_cmd = get_cmd + f' {ff}'
        get_cmd += f' {self.get_dir}'
        subprocess.run(get_cmd, shell=True, 
                       stdout=subprocess.DEVNULL, 
                       stderr=subprocess.STDOUT)
        
        # Save list of current temporary files
        got_files = []
        for ff in self.files_batch:
            got_files.append(path.join(self.get_dir, path.basename(ff)))
            
        self.tmp_files = self.tmp_files + got_files
        
    def put_batch(self):  
        # Create get command using gsutil and run from command line
        put_cmd = 'gsutil -m cp '
        for ff in self.files_batch:
            put_cmd = put_cmd + f' {ff}'
        put_cmd += f' {self.put_dir}'
        subprocess.run(put_cmd, shell=True, stdout=subprocess.DEVNULL, 
                                stderr=subprocess.STDOUT)
    
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
        
    def delete_tmp_files(self):
        tmp_files = self.tmp_files
        for ff in tmp_files:
            try:
                os.remove(ff)
            except:
                pass
        self.tmp_files = []
            
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
    
    def _make_files_from_components(self, components):
        
        n_components = len(components)
        n_subcomponents = [len(cc) for cc in components]
        n_files = np.prod(n_subcomponents)
        
        tmp_list = components[0]
        
        for ii in np.arange(1,n_components):
            tmp_list = self._iterate_list(tmp_list, n_subcomponents[ii])
            c_ii = components[ii]
            n_ii = n_subcomponents[ii]
            for jj in range(len(tmp_list)):
                tmp_list[jj] = tmp_list[jj] + c_ii[jj%n_ii]
        
        return tmp_list
    
    def _iterate_list(self, inlist, it=2):
        
        n_elements = len(inlist)
        new_list = ['' for ii in range(it*n_elements)]
        
        for ii in range(it):
            new_list[ii::it] = inlist
            
        return new_list
    
    def _gsls(self, path):
        
        cmd = f"gsutil ls {path}"
        output = subprocess.check_output(cmd, shell=True)
        output = str(output)[2:-1]
        
        output_split = output.split('\\n')[:-1]
        
        return output_split
    
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
            
        