# Helpers for batching analysis of google bucket files
Python classes to help with batching functions over data that is obtained
from google buckets using gsutil. 

See notebooks in /notebooks directory for example useage (view them on github).

## Installation

You can install the latest pip version quickly by using `pip install cloudbatch`.

Or install from source by cloning this repo and using `pip install -e .` from inside the top directory.

## Quick Start

This package revolves around classes which are children of the CloudBatch class. These classes take things like lists of files, or arguments required to generate lists of files, file directory and batch size to cut up files into batches. With each batch we can download its files using the `.get_batch()` function and upload them using `.put_batch()`. These two functions are specific to each class.

*Example: to batch files on a remote google bucket.* Import some things:

```
from cloudbatch import GSBatch
```

Suppose we have a list of 50 file names `file_list` on a google bucket, we can create a GSBatch object to retrieve these files in batches:

```
gsbatch = GSBatch(files = file_list, 
                  file_dir = "gs://your_bucket/your_dir",
                  get_dir = "/home/you/data/tmp",
                  source = "remote",
                  batch_size = 10)
```

