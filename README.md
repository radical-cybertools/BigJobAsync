BigJobSimple
============

Lean wrapper script around BigJob that implements transparent file transfers 
and makes heavy use of callbacks.


Installation
------------

This wrapper needs BigJob and SAGA-Python installed:

```bash
virtualenv $HOME/bjenv
source $HOME/bjenv/bin/activate
pip install saga-python
pip install bigjob
```

Running the Example
-------------------

Running the example is pretty straight forward. Edit the `example.py` file 
and change the following lines:

    x

Now you can run the example:

    python example.py

Resources
---------

Resource configurations are defined in https://github.com/oleweidner/BigJobSimple/blob/master/bjsimple/resource_dictionary.py.

You reference a resource configuration via the `resource` parameter in the `Resource` class constructor:

```python
stampede = bjsimple.Resource(
    resource   = bjsimple.RESOURCES['XSEDE.STAMPEDE'], 
```

All resource configuration define a default queue on the remote system. You can override it by setting the  `queue` parameter explicitly in the `Resource` class constructor.

If you want to add a new resource, just add it to the `resource_dictionary.py` file.