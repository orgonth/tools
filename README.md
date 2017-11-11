# tools
Few unrelated utility tools.
Feel free to use them, improve them, and share your improvements!

* multi.py -- Multiple threads manager with groups and group-priority. This module provides a pool-like class (ThreadManager) similar to what you can find in
the builtin multiprocess module, however it uses threads instead of subprocesses.

* unify.py -- Combines source code of local modules into a single file. The purpose is to get a single .py file that includes the source code of all imported modules found in the local folder. The output code can then be cythonized, for example, to get a single pyd file. Imported modules will still be accessible through the main module as usual.
