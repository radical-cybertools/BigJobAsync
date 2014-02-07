# BigJobAsync

Asynchronous wrapper around BigJob that implements transparent file transfers
and makes heavy use of callbacks and multiprocessing to speed up transfers in
the background.

## Requirements

* Python >= 2.5

## Installation

### 1. Create a Python Virtualenv

Now we can use it to create a new `MDStack` virtualenv (use a different name if you want):

```
virtualenv $HOME/MDStack
source $HOME/MDStack/bin/activate
```

> **NOTE:** If you don't have virtualenv installed, you can run it like this:
```
wget --no-check-certificate https://pypi.python.org/packages/source/v/virtualenv/virtualenv-1.9.tar.gz
tar xzf virtualenv-1.9.tar.gz
python virtualenv-1.9/virtualenv.py $HOME/MDStack
source $HOME/MDStack/bin/activate
```

### 2. Install BigJobAsync

Now that we have a local virtualenv, we can install any Python packages without requiring root privileges. We install the latest stable version of BigJobAsync directly from GitHub. The installer will automatically install all required dependencies, including BigJob and SAGA-Python:

```
pip install --upgrade -e git://github.com/radical-cybertools/BigJobAsync.git@master#egg=bigjobasync
```

Once the installer has finished, make sure everything is in place (version numbers might diverge):

```
python -c "import saga; print saga.version"
0.10
python -c "import bigjob; print bigjob.version"
0.64.5
python -c "import bigjobasync; print bigjobasync.version"
0.3
```

### 3. Set Up Access Credentials for Stampede

In order to use BigJobAsync with stampede, we need to set up password-less SSH login. First, we create a new RSA keypair.

> **NOTE:** Make sure that you save it as **/home/username/.ssh/mdstack_rsa** and that you leave the password empty

```
ssh-keygen -t rsa -C "MDStack" 

Generating public/private rsa key pair.
Enter file in which to save the key (/home/oweidner/.ssh/id_rsa): /home/oweidner/.ssh/mdstack_rsa
Enter passphrase (empty for no passphrase): 
Enter same passphrase again: 
Your identification has been saved in /home/oweidner/.ssh/mdstack_rsa.
```

Next, in a separate terminal window, log in to your account on stampede and add the content of the newly generated `/home/username/.ssh/mdstack_rsa.pub` to `$HOME/.ssh/authorized_keys` on stampede. Once that's done, you can close the connection. 

Back on the UCL lab machine, try to log in to stampede with your new key:

```
ssh -i $HOME/.ssh/mdstack_rsa tacc_username@stampede.tacc.utexas.edu
```

Once that works, create a file `$HOME/.ssh/config` and add the following entry:

```
Host *.tacc.utexas.edu
IdentityFile ~/.ssh/mdstack_rsa
User tacc_username
```

Now you should be able to login to stampede without providing a username or an identity: 

```
ssh stampede.tacc.utexas.edu
```

If that works, you are all set. 

### 4. Run the Example Script 

Create a directory `$HOME/example` and download / copy the sample input files and example script to it.

```
mkdir $HOME/example
cd $HOME/example
wget https://raw.github.com/radical-cybertools/BigJobAsync/master/examples/loreipsum_pt1.txt
wget https://raw.github.com/radical-cybertools/BigJobAsync/master/examples/loreipsum_pt2.txt
wget https://raw.github.com/radical-cybertools/BigJobAsync/master/examples/01_example_local_input.py
```

Open the file `01_example_local_input.py` and change the following lines.

```Python
# CHANGE: Your stampede username
USERNAME    = "tg802352" 
# CHANGE: Your stampede working directory 
WORKDIR     = "/scratch/00988/tg802352/example/"
# CHANGE: Your stampede allocation
ALLOCATION  = "TG-MCB090174"
```

Now you can run the script

```
python 01_example_local_input.py
```

The output should look similar to the one below, however, there won't be any particular order as the individual stages of task execution run interleaved and highly asynchronously.

```
 * Task combinator-task-0 state changed from 'New' to 'TransferringInput'.
 * Task combinator-task-1 state changed from 'New' to 'TransferringInput'.
[...]
 * Resource '<_BigJobWorker(_BigJobWorker-9, started daemon)>' state changed from 'New' to 'Pending'.
 * Task combinator-task-0 state changed from 'TransferringInput' to 'WaitingForExecution'.
 * Task combinator-task-1 state changed from 'TransferringInput' to 'WaitingForExecution'.
[...]
 * Task combinator-task-0 state changed from 'WaitingForExecution' to 'Pending'.
 * Task combinator-task-1 state changed from 'WaitingForExecution' to 'Pending'.
[...]
 * Resource '<_BigJobWorker(_BigJobWorker-9, started daemon)>' state changed from 'Pending' to 'Running'.
[...]
 * Task combinator-task-0 state changed from 'Pending' to 'Running'.
 * Task combinator-task-1 state changed from 'Pending' to 'Running'.
[...]
 * Task combinator-task-0 state changed from 'Running' to 'WaitingForOutputTransfer'.
 * Task combinator-task-1 state changed from 'Running' to 'WaitingForOutputTransfer'.
[...]
 * Task combinator-task-0 state changed from 'TransferringOutput' to 'Done'.
 * Task combinator-task-1 state changed from 'TransferringOutput' to 'Done'.
```
