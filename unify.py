"""
-----
Unify
-----

Tommy Carozzani, 2017

Combines source code of local modules into a single file.
The purpose is to get a single .py file that includes the source code of all imported
modules found in the local folder.
The output code can then be cythonized, for example, to get a single pyd file.
Imported modules will still be accessible through the main module as usual.

Works with:

import mod1, mod2
import mod3 as m
from mod4 import func1, func2
from mod5 import *

Does not support importlib functions

Example:
--------
You can 'unify' a file like this one:

# main.py
import my_submodule
import my_other_mod as mom
from my_third_mod import func3
... do domething...

With the command :

python unify.py main.py unified_module.py

You will then get a single file unified_module that holds everything and that you can compile
with Cython. You may then do something like:

# client.py
import unified_module
unified_module.my_submodule.func1(...)
unified_module.mom.func2(...)
unified_module.func3(...)
"""

import os
import importlib

prefix = '__ufy_'

def _indent(msg, tab='    '):
    """Add indentation to a message line per line"""
    res = ''
    for line in msg.split('\n'):
        res += tab+line+'\n'
    return res

def _nindent(msg, n, char=' '):
    """Add custom indentation to a line"""
    tab = ''
    for i in range(n):
        tab += char
    return tab+msg

def _get_all_funcs(src_fname):
    """
    Get a list of all objects defined in a module.
    This function is called when found something (bad) like :
    from module import *
    Since we don't know objects name, we have to really import the module and find it out.
    """
    curdir = os.getcwd()
    abs_name = os.path.abspath(src_fname)
    dir_name = os.path.dirname(abs_name)
    mod_name = os.path.basename(abs_name).rpartition('.')[0]

    os.chdir(dir_name)
    mod = importlib.import_module(mod_name)

    res = [x for x in dir(mod) if x not in
           ['__builtins__', '__cached__', '__doc__',
            '__file__', '__loader__', '__name__',
            '__package__', '__spec__']
           ]
    os.chdir(curdir)
    return res

def _unify(src_fname, mod_name=None, alias_name=None, raw_namespace_name=None, level=0, stack=[]):
    """
    Reads and return source code of a .py file + local imported modules
    (Recursive function)
    """
    if mod_name is None:
        mod_name = os.path.basename(src_fname).rpartition('.')[0]
    if alias_name is None:
        alias_name = mod_name
    stack.append( (mod_name,alias_name,raw_namespace_name) )
    
    res = ''

    with open(src_fname,'r') as src_in:
        line_nb = 0
        funcs = []
        for line in src_in:
            line_nb += 1
            lstrip = line.strip()
            
            mods = []               # imported modules
            funcs = []              # imported functions ('from ... import funcs')
            from_import = False     # is it an explicit function import? ('from ... import funcs')
            
            if lstrip[:7]=='import ':
                tab, tmp, mods = line.partition('import')
                mods = mods.strip().split(',')
            elif lstrip[:5]=='from ':
                tab, tmp, mods = line.partition('from')
                mods, tmp, funcs = mods.partition('import')
                mods = [mods.strip()]
                funcs = funcs.strip().split(',')
                from_import = True

            if len(mods)>0:
                for mod_name in mods:
                    mod_name = mod_name.strip()
                    tmp = mod_name.partition(' as ')
                    mod_name = tmp[0].strip()
                    if tmp[2]:
                        mod_alias = tmp[2].strip()
                    else:
                        mod_alias = mod_name

                    mod_filename = mod_name+'.py'
                    
                    if os.path.isfile(mod_filename):
                        # We can include mod's code
                        
                        already_imported=False
                        for previous_mod_name, previous_mod_alias, previous_raw_namespace in stack[1:]:
                            if mod_name==previous_mod_name:
                                res += _indent('# import {} as {} -- skipped'
                                              .format(mod_name, mod_alias),tab)
                                res += _indent('{}={}'
                                              .format(mod_alias,previous_raw_namespace),tab)
                                already_imported=True
                                break
                        if already_imported:
                            continue
                        
                        print(_nindent('[{}] line: {} --- include {} ---'
                                      .format(src_fname, line_nb, mod_filename),
                                      level))

                        if len(funcs)>0:
                            title = 'from {} import {}'.format(mod_name,funcs[0])
                            if len(funcs)>1:
                                for f in funcs[1:]:
                                    title += f', {f}'
                        elif mod_alias != mod_name:
                            title = 'import {} as {}'.format(mod_name,mod_alias)
                        else:
                            title = 'import {}'.format(mod_name)

                        if not from_import:
                            namespace_name = mod_alias
                            raw_namespace_name = '{}raw_{}'.format(prefix,mod_name)
                        else:
                            namespace_name = '{}from_{}'.format(prefix,mod_alias)
                            raw_namespace_name = namespace_name
                        

                        import_func_name = '{}import_{}_as_{}'.format(prefix, mod_name, mod_alias)

                        # Internal routines
                        res += _indent('#####',tab)
                        res += _indent('# '+title,tab)
                        res += _indent('__name__="{}"'.format(mod_name),tab)

                        # Declare the namespace
                        if namespace_name!=raw_namespace_name:
                            res += _indent('{}={}={}Namespace()'
                                          .format(namespace_name,raw_namespace_name,prefix), tab)
                        else:
                            res += _indent('{}={}Namespace()'
                                          .format(namespace_name,prefix),tab)

                        # Function that will define the content of the namespace (definition function)
                        res += _indent('def {}():'.format(import_func_name),tab)

                        # Include the code of the module in the definition function
                        res += _indent(_indent( _unify(mod_filename, mod_name, mod_alias,
                                                    raw_namespace_name, level+1, stack.copy())
                                             ),tab)

                        # Declare the module's functions/class/objects to live in the namespace scope
                        f_src = 'for __f in dir():\n'
                        f_src+= '    if __f[:{}]!="{}":\n'.format(len(prefix),prefix)
                        f_src+= '        setattr({},__f,eval(__f))'.format(raw_namespace_name)
                        res += _indent(_indent(f_src),tab)

                        # Call to the definition function
                        res += _indent('{}()'.format(import_func_name),tab)

                        # Declare the explicitely imported functions to live in the current scope
                        if '*' in funcs:
                            funcs = _get_all_funcs(mod_filename)
                        for f in funcs:
                            res += _indent('{}={}.{}'.format(f,namespace_name,f),tab)

                        # Some cleanup (so internal objects are hidden)
                        res += _indent('del({})'.format(import_func_name),tab)
                        if not from_import:
                            res += _indent('del({})'.format(raw_namespace_name),tab)

                        # End
                        if level==0:
                            parent_name = '__main__'
                        else:
                            parent_name = mod_name
                        res += _indent('__name__="{}"'.format(parent_name),tab)
                        res += _indent('# end '+title,tab)
                        res += _indent('#####',tab)
                    else:
                        if from_import:
                            res += line
                        else:
                            if mod_alias!=mod_name:
                                exp = 'import {} as {}'.format(mod_name,mod_alias)
                            else:
                                exp = 'import {}'.format(mod_name)
                            res += _indent(exp,tab)
            else:
                res += line
    return res

def merge_imports(filename_in, filename_out):
    """
    Main function to process a file
    """
    fout = open(filename_out, 'w')
    fout.write('class {}Namespace:\n'.format(prefix))
    fout.write('    pass\n\n')
    fout.write(_unify(filename_in))
    fout.close()
    
if __name__=='__main__':
    import sys
    if len(sys.argv)==3:
        merge_imports(sys.argv[1],sys.argv[2])
    else:
        print('Argument error.')
