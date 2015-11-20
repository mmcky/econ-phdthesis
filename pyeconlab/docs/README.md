PyEconLab Documentation
=======================

This is the documentation for PyEconLab

Currently this is a built document. The following command should be issued at the root level of the repo: 

``sphinx-apidoc -o docs pyeconlab``.

**Note:** ``-f`` may need to be specified to overwrite the generated rst pages

``PyEconLab`` Docstring Style
-----------------------------

The ``PyEconLab`` style largely follows that of ``numpydoc`` and other scientific python items. 
However during development it can be helpful to have a few extra fields (i.e. Future Work).
This can be added using a RestructuredText comment until we develop a custom version of ``sphinxext.napoleon``. 

```
.. 	Future Work
	-----------
	#. Item 1 should be written here etc. 
```

Note this is ignored when compiling the documentation using Sphinx

Docstring Reminders
-------------------

To conform with ``NumPy`` documentation the following should be followed. 

Deprecation Warnings should we added as sphinx ``note`` directive

```
.. note:: Deprecated in PyEconLab 0.1
	`function` will be removed in PyEconLab 0.2, as it will be replaced by `newfunction`
```

Parameters for Functions and Methods

```
Parameters
----------
x : type
    Description of parameter `x`.
```

You cannot use section titles within docstrings. Sphinx and Autodoc will drop these when compiling the docstring.
Instead you may use:

```
**Notes**:
  This is an example note
```

**Note**: This looks nice in the read-the-docs theme

or alternatively use a sphinx directive

```
.. note::
	This is an example note
```

**Note**: This ends up being very pronounced in the read-the-docs theme - and useful for setting warnings or important notes in the documentation

or if using the ``sphinxcontrib-napoleon`` extension then you can use special [section headers](https://pypi.python.org/pypi/sphinxcontrib-napoleon#sections) like

```
Notes
-----
#. This is an examle note
```

**Note**: This is the most similar to the current convention used in ``pyeconlab`` and is the most straighforward to implement

Future Work
-----------
  1. Construct a ``make.py`` file for building the documentation (similar to Pandas). This may or may not replace the sphinx MakeFile

References
----------
  1. [NumPy Documentation Style Guide](<https://github.com/numpy/numpy/blob/master/doc/HOWTO_DOCUMENT.rst.txt>)
  1. [Markdown Syntax](http://daringfireball.net/projects/markdown/syntax)
  1. [RestructuredText Quick Reference](http://docutils.sourceforge.net/docs/user/rst/quickref.html)
  1. [Sphinx DocString Tutorial](http://thomas-cokelaer.info/tutorials/sphinx/docstring_python.html)