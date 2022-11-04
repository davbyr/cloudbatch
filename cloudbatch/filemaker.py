import numpy as np

def file_list_from_components(components, file_ext='', join_str='_'):
    ''' Create list of files from components.
    Input should be a list of lists or tuples.
    Each element of components is a list to iterate over for
    that specific component of a file name. For example:
    
    component1 = ['a','b']
    component2 = [1, 2, 3]
    component3 = ['A']
    components = (component1, component2, component3)
    
    file_list_from_components(components, file_ext='nc', join_str='_') 
    
    will give:
    
    ['a_1.nc', 'a_2.nc', 'a_3.nc', 'b_1.nc', 'b_2.nc', 'b_3.nc']
    
    NOTE even if you have a component with just one subcomponent
    (e.g. ['.nc'] above), it should still be provided in a list.
    
    '''

    n_components = len(components)
    n_subcomponents = [len(cc) for cc in components]
    n_files = np.prod(n_subcomponents)
    file_list = components[0]

    for ii in np.arange(1,n_components):
        file_list = _iterate_list(file_list, n_subcomponents[ii])
        c_ii = components[ii]
        
        # Convert all components to string
        c_ii = [str(ss) for ss in c_ii]
        
        # Add join string to beginning of remaining components
        c_ii = ['_' + ss for ss in c_ii]
        
        n_ii = n_subcomponents[ii]
        for jj in range(len(file_list)):
            file_list[jj] = file_list[jj] + c_ii[jj%n_ii]
            
    file_list = [fn + f'.{file_ext}' for fn in file_list]

    return file_list

def _iterate_list(inlist, it=2):
    ''' TODO: write docstring '''
        
    n_elements = len(inlist)
    new_list = ['' for ii in range(it*n_elements)]
        
    for ii in range(it):
        new_list[ii::it] = inlist
            
    return new_list