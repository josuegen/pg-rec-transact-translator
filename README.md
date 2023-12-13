# Auntonomous transaction converter

## Motivation
The tools in the market are not available to translate the Oracle autonomous transactions. Then, we decided to automate the process to change non autonomous transactions PG functions/procedures
to an approach that mimics the behavior of the autonomous transactions in Oracle, this consists on:
1. Rename a `original_function()` to something like `xx_original_function()`, where `xx` ccoul be any prefix.
2. Inside thge original function, open a dblink with a remote server already setup as a prerequisite
* Prepare the arguments
* Prepare the statement
3. Call the `xx_original_function()` inside the `original_function()` to make it run through the already opened dblink connection.

## Constraints
1. The base librarie `pglast` works only in Unix environments
2. The script needs to have access to the DDL SQL scripts through a standard file system

## Usage
### Create the container
Install the required libraries 
```bash
pip install -r requirements.txt
```

### Run the container
Execute the script passing the required arguments
```bash
python3 src/autonomous_transaction_converter.py \
    --input_path some/local/path \
    --output_path some/other/path
```