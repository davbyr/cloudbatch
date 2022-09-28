import xarray as xr
import subprocess
import numpy as np

class Gsbatch():
    
    def __init__(self, 
                 gs_list = None, gs_components = None,
                 gs_getdir = '',
                 local_list = None, local_components = None,
                 local_ext = '.nc',
                 batch_size=10
                ):

        # Define GS files
        if gs_components is not None:
            n_components = len(gs_components)
            gs_files = self._make_files_from_components(gs_components)
        elif gs_list is not None:
            gs_files = gs_list
        else:
            raise Exception("You need to provide file_list or file_components")
            
        # Define local files
        if local_components is not None:
            n_components = len(local_components)
            local_files = self._make_files_from_components(local_components)
        elif local_list is not None:
            local_files = local_list
        else:
            local_files = None
        
        # Initialise batches
        self.current_file = 0
        self.current_batch = 0
        
        n_files = len(gs_files)
        
        if local_files is not None:
            if len(local_files) != n_files:
                raise Exception('Number of GS Files does not match local files')
        
        n_batches = np.ceil( n_files / batch_size ).astype(int)
        last_batch_size = n_files % batch_size
        
        self.n_batches = n_batches
        self.n_files = n_files
        self.gs_files = gs_files
        self.local_files = local_files
        self.last_batch_size = last_batch_size
        self.batch_size = batch_size
        self.gs_getdir = gs_getdir
        
        self._update_batch()
        
        # Set get script command
        get_cmd = 'gsutil -m cp {0} {1}'
        
        return
            
    def next_batch(self):
        self.current_batch = self.current_batch + 1
        self.current_file = self.current_file + self.batch_size
        self._update_batch()
        
    def prev_batch(self):
        if self.current_batch > 0:
            self.current_batch = self.current_batch - 1
            self.current_file = self.current_file - self.batch_size
            self._update_batch()
        
    def reset_batch(self):
        self.current_batch = 0
        self.current_file = 0
        self._update_batch()
        return
    
    def get_batch(self):
        
        return
        
    def put_batch(self):
        return
    
    def set_batch_size(self, batch_size):
        
        self.batch_size = batch_size
        n_batches = np.ceil( self.n_files / batch_size ).astype(int)
        last_batch_size = self.n_files % batch_size
        
        self.n_batches = n_batches
        self.last_batch_size = last_batch_size
    
    def _update_batch(self):
        
        start_idx = self.current_file
        
        if self.current_batch < self.n_batches - 1:
            end_idx = self.current_file + self.batch_size
        else:
            end_idx = self.current_file + self.last_batch_size
        
        self.gs_files_batch = self.gs_files[start_idx:end_idx]
        
        if self.local_files is not None:
            self.local_files_batch = self.local_files[start_idx:end_idx]
    
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
        
        
            
        