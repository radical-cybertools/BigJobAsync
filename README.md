BigJobSimple
============

Lean wrapper script around BigJob that implements transparent file transfers, 
and makes heavy use of callbacks and multiprocessing to speed up transfers in the background. 


Installation
------------

This wrapper needs BigJob and SAGA-Python installed:

```bash
virtualenv $HOME/bjenv
source $HOME/bjenv/bin/activate
pip install saga-python
pip install bigjob
```

Next, check out the `BigJobSimple` repository:

```bash
git clone https://github.com/oleweidner/BigJobSimple.git
```

Running the Example
-------------------

Running the example is pretty straight forward. Edit the `example.py` file 
and change the following settings:

* base working directory: https://github.com/oleweidner/BigJobSimple/blob/master/example.py#L71
* project / allication id: https://github.com/oleweidner/BigJobSimple/blob/master/example.py#L72

Put the input files somewhere and adjust the input transfer directives:

* https://github.com/oleweidner/BigJobSimple/blob/master/example.py#L130
* https://github.com/oleweidner/BigJobSimple/blob/master/example.py#L136

NOTE: You can find copies of the input files here:

* https://gist.github.com/oleweidner/7711750
* https://gist.github.com/oleweidner/7711766
    
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
