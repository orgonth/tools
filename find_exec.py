import subprocess
import os

def find_exec(exe_name, additional_dirs=[]):
    """Find if an executable is present within the PATH

       :param exe_name: name of the executable to find, or a list of different names it can take
       :param additional_dirs: list of additional directories to look into (with higher priority than PATH)
       :return: path of the executable if found, None otherwise
    """
    if type(exe_name)==str:
        exe_name = (exe_name,)

    exe_dirs = additional_dirs + [os.getcwd()] + os.get_exec_path()
    for d in exe_dirs:
        for x in exe_name:
            if os.path.exists(d+'/'+x):
                return os.path.abspath(d+'/'+x)
    return None
