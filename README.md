BigJobAsync
===========

Asynchronous wrapper around BigJob that implements transparent file transfers
and makes heavy use of callbacks and multiprocessing to speed up transfers in
the background.

Requirements
------------

* Python >= 2.5

Installation
------------

BigJobAsync can be installed with pip directly from GitHub:

```bash
virtualenv $HOME/bjaenv
source $HOME/bjaenv/bin/activate
pip install --upgrade -e git://github.com/radical-cybertools/BigJobAsync.git@master#egg=bigjobasync
```

The installer automatically installed the latest versions of SAGA-Python (http://saga-project.github.io/saga-python/)
and BigJob (http://saga-project.github.io/BigJob/).

Running the Examples
--------------------

The examples are in the 'examples' directory (https://github.com/radical-cybertools/BigJobAsync/blob/master/examples/).

Check out the examples and adapt the resource configuration (e.g., `username`, `workdir`, `project`) to your specific needs:

```python
    stampede = bigjobasync.Resource(
        name       = "stampede:16cores", 
        resource   = bigjobasync.RESOURCES['XSEDE.STAMPEDE'],
        username   = "tg802352",
        runtime    = 2, 
        cores      = 16, 
        workdir    = "/scratch/00988/tg802352/example/",
        project_id = "TG-MCB090174"
    )
```


Resources
---------

Resource configurations are defined in https://github.com/radical-cybertools/BigJobAsync/blob/master/bjsimple/resource_dictionary.py.

You reference a resource configuration via the `resource` parameter in the
`Resource` class constructor:

```python
stampede = bjsimple.Resource(
    resource   = bjsimple.RESOURCES['XSEDE.STAMPEDE'], 
```

All resource configuration define a default queue on the remote system. You
can override it by setting the  `queue` parameter explicitly in the `Resource`
class constructor.
