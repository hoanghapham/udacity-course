
### Install Python 3.7.14
You should install an alternative Python version via `pyenv`. AWS EMR release `5.36.0` has Python `3.7.15`, but this version is not availabe via `pyenv`, we can use version `3.7.14` instead.

Assuming that you already have pyenv installed, run the following:

```bash
pyenv install 3.7.14
```

On PopOS, you may encounter this error: `python-build: line xxx: xxxx Segmentation fault`. If that's the case, run the following:

```bash
sudo apt install clang -y;
CC=clang pyenv install 3.7.14;
```
(Reference: https://github.com/pyenv/pyenv/issues/2239)


Then cd in to the project folder, run this to set the project's python version to Python 3.7.14
```bash
pyenv local 3.7.14
```

### Install required packages:

```bash
pip3 install -r requirements.txt;
```
