import numpy as np

class CloudBatch_apply():
    
    def __init__(self, func, batch, 
                 verbosity = 0,
                 pass_args = "one",
                 delete_put_files = False):

        if type(batch) is not list:
            batch = [batch]

        n_args = len(batch)

        if verbosity > 0:
            print(f"  Applying function {func.__name__} to {n_args} cloudbatch object.")
            print("")

        # Reset all batches
        if verbosity > 0: print("   --> Resetting all batches")
        [bb.reset_batch() for bb in batch]

        # Check number of batches are aligned
        if not np.all( [bb.n_batches for bb in batch] ):
            raise Exception("n_batches does not match between input cloudbatch objects.")
                      
        batch_size = batch[0].batch_size
        n_batches = batch[0].n_batches
        if verbosity > 0: print(f"   --> Number of batches: {n_batches}")
                    
        # Are we getting or putting any data?
        sources = [bt.source for bt in batch]
        get_bool = np.array( [ss == 'remote' for ss in sources] ).astype(bool)
        put_bool = np.array( [ss == 'local' for ss in sources] ).astype(bool)
        n_gets = np.sum(get_bool)
        n_puts = np.sum(put_bool)
        
        if n_gets == 0 and n_puts == 0:
            raise Exception(" You are not getting or putting any data so why use cloudbatch? ")

        # Now start the cycle of going through batches and passing to the function
        all_out = []
        for bb in range(n_batches):

            if verbosity == 1:
                print(f"   --> Processing batch: {batch[0].current_batch + 1} / {batch[0].n_batches}")
                
            # Download the data if source is remote
            if n_gets > 0:
                if verbosity == 2: 
                    print(f"      --> Getting data from {n_gets} cloudbatch objects.")
                [bt.get_batch() for bt in batch if bt.source == 'remote']

            if pass_args == 'one':
                if verbosity ==2: print(f"      --> Applying function one file at a time.")
                batch_out = self._apply_one_at_a_time(func, batch)
            elif pass_args == 'all':
                if verbosity ==2: print(f"      --> Applying function to all files in batch.")
                batch_out = self._apply_all_at_once(func, batch)
            else:
                raise Exception("Unrecognised pass option. Choose: pass_args = ['one','all']")
                
            # Stick the outputs onto the end of the current outputs
            all_out.append(batch_out)
            
            # Upload the data if source is local
            if n_puts > 0:
                if verbosity == 2: print(f"      --> Uploading data to {n_puts} cloudbatch objects.")
                [bt.put_batch() for bt in batch if bt.source == 'local']
                
            # Delete any downloaded files
            [bt.delete_tmp_files() for bt in batch]
            
            for bt in batch:
                if delete_put_files and bt.source == 'local':
                    bt.delete_batch_files()
                
            
            # Cycle up batches
            [bt.next_batch() for bt in batch]
        
        self.output = all_out
        if verbosity ==1: print('Done! Phew.')
                 

    def _apply_one_at_a_time(self, func, batch):
        
        # Get files differently depending on source
        output = []

        # Handle slightly differently if more than one batch vs one batch
        if len(batch) > 1:
            
            batch_files = []
            for bb in batch:
                if bb.source == 'remote':
                    batch_files.append(bb.tmp_files)
                else:
                    batch_files.append(bb.files_batch)
                    
            n_files = len(batch_files[0])
            n_args = len(batch)

            for ff in range(n_files):
                # Make list of input files
                args = [batch_files[ii][ff] for ii in range(n_args)]
                output.append( func(*args) )
        else:
            tmp_files = batch[0].tmp_files
            for ff in range(n_files):
                output.append( func( batch_files[ff] ) )
        
        
        return output

    def _apply_all_at_once(self, func, batch):
        raise NotImplemented(" For future development . . . ")