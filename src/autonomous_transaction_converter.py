# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

import os
import sys
import copy
import pglast
import argparse
from typing import List, Tuple
from pglast.stream import RawStream



def get_current_directory() -> str:
    return os.getcwd()


def get_files_to_process(pg_scripts_path: str) -> List[str]:
    """Lists of all the SQL files contained in a directory

    Args:
        pg_scripts_path (str): Path containing the source scrips

    Returns:
        List[str]: List of SQL files in source path directory
    """

    files_in_dir = list_sql_files_in_dir(pg_scripts_path=pg_scripts_path)

    autotran_scripts = filter_sql_files_autotrans(
        pg_scripts_path = pg_scripts_path,
        scripts_list = files_in_dir
    )

    return autotran_scripts


def list_sql_files_in_dir(pg_scripts_path: str) -> List[str]:
    """_summary_

    Args:
        pg_scripts_path (str): _description_

    Returns:
        List[str]: _description_
    """

    files = [
        f for f in os.listdir(pg_scripts_path)
        if f.endswith('.sql')
    ]

    print(f'Total SQL files in path: {len(files)}')

    return files


def filter_sql_files_autotrans(pg_scripts_path: str, scripts_list: str) -> List[str]:

    autotrans_scripts_count = 0
    auto_tran_file_list = []

    for file in scripts_list:
        content = ''

        with open(pg_scripts_path + file, 'r') as f:
            content = f.read()
            if 'AUTONOMOUS_TRANSACTION' in content.upper():
                autotrans_scripts_count += 1
                auto_tran_file_list.append(pg_scripts_path + file)

    print(f'AUTONOMOUS_TRANSACTION SQL files in path: {autotrans_scripts_count}')

    return auto_tran_file_list


def parse_query_file(query_file: str) -> Tuple[str]:

    with open(query_file, 'r') as f:
        content = f.read()
        parse_tree = pglast.parser.parse_sql(content)

    obj_name = parse_tree[0].stmt.funcname[-1].sval

    if not obj_name:
        raise Exception(f'Could not parse {query_file}')
    
    return parse_tree


def find_createstmt_node(parse_tree: Tuple[str]) -> int:

    create_func_stmt_node_num = -1

    for node_num in range(len(parse_tree)):
        if isinstance(parse_tree[node_num].stmt, pglast.ast.CreateFunctionStmt):
            create_func_stmt_node_num = node_num

    if create_func_stmt_node_num < 0:
        raise Exception(f'There are no instances of pglast.ast.CreateFunctionStmt in {parse_tree}')

    return create_func_stmt_node_num


def build_caller_query(parse_tree: Tuple[str]) -> str:
    createfunction_stmt_index = find_createstmt_node(parse_tree = parse_tree)
    stmt = parse_tree[createfunction_stmt_index].stmt

    obj_parameters = ''
    obj_parameters_names = ''
    formatted_obj_parameters_names = ''

    current_directory = get_current_directory()

    if stmt.is_procedure:
        ddl_balseline_file = open(f'{current_directory}/procedure_ddl_baseline.txt','r')
    else:
        obj_return_type = RawStream()(stmt.returnType)
        ddl_balseline_file = open(f'{current_directory}/function_ddl_baseline.txt','r')

    ddl_balseline = ddl_balseline_file.read()
    ddl_balseline_file.close()
        
    if stmt.replace:
        obj_action = 'CREATE OR REPLACE'
    else:
        obj_action = 'CREATE'

    obj_schema =  stmt.funcname[0].sval
    obj_name =  stmt.funcname[-1].sval

    if stmt.parameters:
        obj_parameters = RawStream()(stmt.parameters)
        obj_parameters = obj_parameters.replace(';',',')
    
        obj_parameters_names_list = [
            f"{parameter.name}"
            for parameter in stmt.parameters
        ]
        obj_parameters_names = ', ' + ', '.join(obj_parameters_names_list)

        formatted_obj_parameters_names_list = [
            f"{parameter.name} => %L"
            for parameter in stmt.parameters
        ]
        formatted_obj_parameters_names = ', '.join(formatted_obj_parameters_names_list)

    if stmt.is_procedure:
        ddl_caller_query = ddl_balseline.format(
            obj_action = obj_action,
            obj_schema = obj_schema,
            obj_name = obj_name,
            obj_parameters = obj_parameters,
            obj_parameters_names = obj_parameters_names,
            formatted_obj_parameters_names = formatted_obj_parameters_names,
        )
    else:
        ddl_caller_query = ddl_balseline.format(
            obj_action = obj_action,
            obj_schema = obj_schema,
            obj_name = obj_name,
            obj_parameters = obj_parameters,
            obj_parameters_names = obj_parameters_names,
            formatted_obj_parameters_names = formatted_obj_parameters_names,
            obj_return_type = obj_return_type
        )

    pretty_ddl_caller_query = pglast.prettify(ddl_caller_query)

    if not pretty_ddl_caller_query.endswith(';'):
        pretty_ddl_caller_query += ';'

    return pretty_ddl_caller_query


def get_renamed_source_query(parse_tree: Tuple[str]) -> str:
    createfunction_stmt_index = find_createstmt_node(parse_tree=parse_tree)

    stmt = parse_tree[createfunction_stmt_index].stmt
    mutable_stmt = copy.deepcopy(stmt)
    new_name = f'xx_{stmt.funcname[-1].sval}'

    mutable_stmt.funcname = (stmt.funcname[0], new_name)
    
    str_mutable_stmt = pglast.prettify((RawStream()(mutable_stmt)))

    if not str_mutable_stmt.endswith(';'):
        str_mutable_stmt += ';'

    return str_mutable_stmt


def is_multi_statement_ddl(parse_tree: Tuple[str]) -> bool:
    if len(parse_tree)>1:
        return True
    
    return False


def deparse_remaining_ddl(parse_tree: Tuple[str]) -> str:
    deparsed_ddls_list = []

    create_func_stmt_index = find_createstmt_node(parse_tree = parse_tree)
    for node_num in range(len(parse_tree)):
        if node_num != create_func_stmt_index:
            deparsed_ddl = pglast.prettify(RawStream()(parse_tree[node_num]))
            if not deparsed_ddl.endswith(';'):
                deparsed_ddl += ';'
            deparsed_ddls_list.append(deparsed_ddl)
    
    deparsed_ddls = '\n'.join(deparsed_ddls_list)

    return deparsed_ddls


def get_arguments():
    parser = argparse.ArgumentParser()
  
    parser.add_argument(
        '--input_path',
        type = str,
        help = 'Absolute path of the Postgres source SQL files',
        required = True
    )

    parser.add_argument(
        '--output_path',
        type = str,
        help = 'Absolute path of the destination of the converted files',
        required = True
    )

    args = parser.parse_args()

    return args


if __name__ == '__main__':
    sys.setrecursionlimit(10000)
    arguments = get_arguments()
    pg_source_scripts_path = arguments.input_path if arguments.input_path.endswith('/') else arguments.input_path + '/'
    pg_converted_scripts_path = arguments.output_path if arguments.output_path.endswith('/') else arguments.output_path + '/'
    
    autotran_scripts = get_files_to_process(pg_scripts_path = pg_source_scripts_path)

    for autotran_script in autotran_scripts:
        try:
            parsed_file = parse_query_file(query_file = autotran_script)
            caller_query = build_caller_query(parse_tree = parsed_file)
            renamed_source_query = get_renamed_source_query(parse_tree = parsed_file)

            final_query = '\n\n\n'.join([caller_query, renamed_source_query])

            if is_multi_statement_ddl(parsed_file):
                deparsed_remaining_ddl = deparse_remaining_ddl(parse_tree = parsed_file)
                final_query = '\n\n'.join([final_query, deparsed_remaining_ddl])

            # '/usr/local/google/home/josuegen/Downloads/transformed/'
            file_out = open(pg_converted_scripts_path + autotran_script.split('/')[-1],'w')
            file_out.write(final_query)
            file_out.close()

        except Exception as e:
            print(f'Could not process: {autotran_script}: {e}')