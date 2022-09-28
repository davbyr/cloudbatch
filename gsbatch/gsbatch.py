import xarray as xr
import subprocess
import numpy as np
import os
import os.path as path
import glob

class Gsbatch():
    
    def __init__(self, 
                 gs_list = None, 
                 gs_components = None,
                 gs_dir = None,
                 search = 'remote',
                 batch_size=10
                ):

        # Define GS files
        if gs_components is not None:
            n_components = len(gs_components)
            gs_files = self._make_files_from_components(gs_components)
        elif gs_list is not None:
            if type(gs_list) is str:
                gs_list = [gs_list]
            gs_files = gs_list
        else:
            raise Exception("You need to provide file_list or file_components")
            
        # Add directory to file names if wanted
        if gs_dir is not None:
            gs_files = [path.join(gs_dir, fn) for fn in gs_files]
            
        # Check for wildcards
        gs_files = self._expand_wildcards(gs_files, search)
        
        # Initialise batches
        self.current_file = 0
        self.current_batch = 0
        
        n_files = len(gs_files)
        
        n_batches = np.ceil( n_files / batch_size ).astype(int)
        last_batch_size = n_files % batch_size
        
        self.n_batches = n_batches
        self.n_files = n_files
        self.gs_files = gs_files
        self.last_batch_size = last_batch_size
        self.batch_size = batch_size
        self.is_last_batch = False
        self.search = search
        self.tmp_files = []
        
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
    
    def get_batch(self, get_dir):  
        
        # Create get command using gsutil and run from command line
        get_cmd = 'gsutil -m cp '
        for ff in self.gs_files_batch:
            get_cmd = get_cmd + f' {ff}'
        get_cmd += f' {get_dir}'
        print(get_cmd)
        subprocess.run(get_cmd, shell=True)
        
        # Save list of current temporary files
        got_files = []
        for ff in self.gs_files_batch:
            got_files.append(path.join(get_dir, path.basename(ff)))
            
        self.tmp_files = self.tmp_files + got_files
        
    def put_batch(self, put_dir):  
        # Create get command using gsutil and run from command line
        put_cmd = 'gsutil -m cp '
        for ff in self.gs_files_batch:
            put_cmd = put_cmd + f' {ff}'
        put_cmd += f' {get_dir}'
        print(put_cmd)
        subprocess.run(put_cmd, shell=True)
    
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
        for ff in self.tmp_files:
            os.remove(ff)
        self.tmp_files = []
            
    def delete_batch_files(self):
        for ff in self.gs_files_batch:
            os.remove(ff)
        self.gs_files_batch = []
        
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
        
        self.gs_files_batch = self.gs_files[start_idx:end_idx]
    
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
        print(cmd)
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
    
    def _expand_wildcards(self, list_of_paths, search):
        
        output_list = []
        n_str = len(list_of_paths)
        
        for ii in range(n_str):
            path = list_of_paths[ii]
            if '*' in path:
                if search == 'remote':
                    wc_files = self._gsls(path)
                elif search == 'local':
                    wc_files = glob.glob(path)
                if type(wc_files) is not list:
                    wc_files = [wc_files]
                output_list = output_list + wc_files
            else:
                output_list.append(path)
                
        return output_list
            
        