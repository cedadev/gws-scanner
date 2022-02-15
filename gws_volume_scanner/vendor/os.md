The os module from the python standard library is patched because os.walk hangs on broken symlinks.

On [this line](https://github.com/python/cpython/blob/e19059ecd80d39dca378cd19f72c5008b1957976/Lib/os.py#L376) follow_symlinks=False is added.
