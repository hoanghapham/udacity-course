# Project 1: Data Modeling with PostgreSQL

In this project, we will model the data of a fictional music-streaming service called Sparkify. 

Diagram for the final model:

<iframe width="560" height="315" src='https://dbdiagram.io/embed/633314907b3d2034ffcc20e5'> </iframe>

## Preparation

This section will walk you through the steps to prepare a PostgreSQL datbase so you can run this modeling project in Linux.

If you use the online workspace provided by Udacity, you can skip this sections. 

### Install PostgreSQL
Follow the instruction here to install PostgreSQL on Linux: https://www.postgresql.org/download/linux/ubuntu/

### Prepare databases
The `create_tables.py` script provided by Udacity will first connect to a default database called `studentdb`, using username `student`. Since we are running PostgreSQL locally, we will need to create the database and the user ourselves. If not, the default connection cannot be initiated.

1. In your terminal, change to `postgres` user:
    ```bash
    sudo su - postgres
    ```
    Provide your user password when prompted. 

2. Once you have logged into the postgres user, run `psql`. Your promt will now look like this:
    ```bash
    postgres=# 
    ```

3. Create the `student` user with `createdb` permission:
    ```sql
    create user student encrypted password 'student' createdb;
    ```

4. Create the `studentdb` database and set `student` as the owner:
    ```sql
    create database studentdb owner student;
    ```

And now you are done.

## Package package dependencies
To install required packages:
```bash
pip3 install -r requirements.txt;
```

## Run the project

Run the following commands in the terminal:

```bash
python3 create_tables.py;
python3 etl.py;

```