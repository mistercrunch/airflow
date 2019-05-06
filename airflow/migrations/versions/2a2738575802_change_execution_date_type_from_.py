#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""change execution_date type from datetime to datetime2(6) for mssql

Revision ID: 2a2738575802
Revises: 939bb1e647c8
Create Date: 2019-05-06 02:07:21.770309

"""

from alembic import op
from sqlalchemy.dialects import mssql
from collections import defaultdict

# revision identifiers, used by Alembic.
revision = '2a2738575802'
down_revision = '939bb1e647c8'
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()
    if conn.dialect.name == "mssql":
        result = conn.execute(
            """SELECT CASE WHEN CONVERT(VARCHAR(128), SERVERPROPERTY ('productversion'))
            like '8%' THEN '2000' WHEN CONVERT(VARCHAR(128), SERVERPROPERTY ('productversion'))
            like '9%' THEN '2005' ELSE '2005Plus' END AS MajorVersion""").fetchone()
        mssql_version = result[0]
        if mssql_version in ("2000", "2005"):
            return

        with op.batch_alter_table('task_reschedule') as task_reschedule_batch_op:
            task_reschedule_batch_op.drop_index('idx_task_reschedule_dag_task_date')

            task_reschedule_batch_op.drop_constraint(
                'task_reschedule_dag_task_date_fkey',
                type_='foreignkey'
            )
            task_reschedule_batch_op.alter_column(
                column_name="execution_date",
                type_=mssql.DATETIME2(precision=6),
                nullable=False,
            )

        with op.batch_alter_table('task_instance') as task_instance_batch_op:
            task_instance_batch_op.drop_index('ti_state_lkp')
            task_instance_batch_op.drop_index('ti_dag_date')

            modify_execution_date_with_constraint(conn, task_instance_batch_op, 'task_instance',
                                                  mssql.DATETIME2(precision=6), False)

            task_instance_batch_op.create_index('ti_state_lkp',
                                                ['dag_id', 'task_id', 'execution_date'], unique=False)
            task_instance_batch_op.create_index('ti_dag_date',
                                                ['dag_id', 'execution_date'], unique=False)

            with op.batch_alter_table('dag_run') as dag_run_batch_op:
                modify_execution_date_with_constraint(conn, dag_run_batch_op, 'dag_run',
                                                      mssql.DATETIME2(precision=6), None)

        with op.batch_alter_table('task_reschedule') as task_reschedule_batch_op:
            task_reschedule_batch_op.create_foreign_key(
                'task_reschedule_dag_task_date_fkey',
                'task_instance',
                ['task_id', 'dag_id', 'execution_date'],
                ['task_id', 'dag_id', 'execution_date'],
                ondelete='CASCADE'
            )
            task_reschedule_batch_op.create_index('idx_task_reschedule_dag_task_date',
                                                  ['dag_id', 'task_id', 'execution_date'], unique=False)

        op.alter_column(
            table_name="log", column_name="execution_date", type_=mssql.DATETIME2(precision=6)
        )

        with op.batch_alter_table('sla_miss') as sla_miss_batch_op:
            modify_execution_date_with_constraint(conn, sla_miss_batch_op, 'sla_miss',
                                                  mssql.DATETIME2(precision=6), False)

        op.drop_index('idx_task_fail_dag_task_date', table_name='task_fail')
        op.alter_column(
            table_name="task_fail",
            column_name="execution_date",
            type_=mssql.DATETIME2(precision=6),
        )
        op.create_index('idx_task_fail_dag_task_date',
                        'task_fail',
                        ['dag_id', 'task_id', 'execution_date'], unique=False)

        op.drop_index('idx_xcom_dag_task_date', table_name='xcom')
        op.alter_column(
            table_name="xcom",
            column_name="execution_date",
            type_=mssql.DATETIME2(precision=6),
        )
        op.create_index('idx_xcom_dag_task_date',
                        'xcom',
                        ['dag_id', 'task_id', 'execution_date'], unique=False)


def downgrade():
    conn = op.get_bind()
    if conn.dialect.name == "mssql":
        result = conn.execute(
            """SELECT CASE WHEN CONVERT(VARCHAR(128), SERVERPROPERTY ('productversion'))
            like '8%' THEN '2000' WHEN CONVERT(VARCHAR(128), SERVERPROPERTY ('productversion'))
            like '9%' THEN '2005' ELSE '2005Plus' END AS MajorVersion""").fetchone()
        mssql_version = result[0]
        if mssql_version in ("2000", "2005"):
            return

        with op.batch_alter_table('task_reschedule') as task_reschedule_batch_op:
            task_reschedule_batch_op.drop_index('idx_task_reschedule_dag_task_date')

            task_reschedule_batch_op.drop_constraint(
                'task_reschedule_dag_task_date_fkey',
                type_='foreignkey'
            )
            task_reschedule_batch_op.alter_column(
                column_name="execution_date",
                type_=mssql.DATETIME,
                nullable=False,
            )

        with op.batch_alter_table('task_instance') as task_instance_batch_op:
            task_instance_batch_op.drop_index('ti_state_lkp')
            task_instance_batch_op.drop_index('ti_dag_date')

            modify_execution_date_with_constraint(conn, task_instance_batch_op, 'task_instance',
                                                  mssql.DATETIME, False)

            task_instance_batch_op.create_index('ti_state_lkp',
                                                ['dag_id', 'task_id', 'execution_date'], unique=False)
            task_instance_batch_op.create_index('ti_dag_date',
                                                ['dag_id', 'execution_date'], unique=False)

            with op.batch_alter_table('dag_run') as dag_run_batch_op:
                modify_execution_date_with_constraint(conn, dag_run_batch_op, 'dag_run',
                                                      mssql.DATETIME, None)

        with op.batch_alter_table('task_reschedule') as task_reschedule_batch_op:
            task_reschedule_batch_op.create_foreign_key(
                'task_reschedule_dag_task_date_fkey',
                'task_instance',
                ['task_id', 'dag_id', 'execution_date'],
                ['task_id', 'dag_id', 'execution_date'],
                ondelete='CASCADE'
            )
            task_reschedule_batch_op.create_index('idx_task_reschedule_dag_task_date',
                                                  ['dag_id', 'task_id', 'execution_date'], unique=False)

        op.alter_column(
            table_name="log", column_name="execution_date", type_=mssql.DATETIME
        )

        with op.batch_alter_table('sla_miss') as sla_miss_batch_op:
            modify_execution_date_with_constraint(conn, sla_miss_batch_op, 'sla_miss',
                                                  mssql.DATETIME, False)

        op.drop_index('idx_task_fail_dag_task_date', table_name='task_fail')
        op.alter_column(
            table_name="task_fail",
            column_name="execution_date",
            type_=mssql.DATETIME,
        )
        op.create_index('idx_task_fail_dag_task_date',
                        'task_fail',
                        ['dag_id', 'task_id', 'execution_date'], unique=False)

        op.drop_index('idx_xcom_dag_task_date', table_name='xcom')
        op.alter_column(
            table_name="xcom",
            column_name="execution_date",
            type_=mssql.DATETIME,
        )
        op.create_index('idx_xcom_dag_task_date',
                        'xcom',
                        ['dag_id', 'task_id', 'execution_date'], unique=False)


def get_table_constraints(conn, table_name):
    """
     This function return primary and unique constraint
     along with column name. some tables like task_instance
     is missing primary key constraint name and the name is
     auto-generated by sql server. so this function helps to
     retrieve any primary or unique constraint name.

     :param conn: sql connection object
     :param table_name: table name
     :return: a dictionary of ((constraint name, constraint type), column name) of table
     :rtype: defaultdict(list)
     """
    query = """SELECT tc.CONSTRAINT_NAME , tc.CONSTRAINT_TYPE, ccu.COLUMN_NAME
     FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
     JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS ccu ON ccu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
     WHERE tc.TABLE_NAME = '{table_name}' AND
     (tc.CONSTRAINT_TYPE = 'PRIMARY KEY' or tc.CONSTRAINT_TYPE = 'Unique')
    """.format(table_name=table_name)
    result = conn.execute(query).fetchall()
    constraint_dict = defaultdict(list)
    for constraint, constraint_type, column in result:
        constraint_dict[(constraint, constraint_type)].append(column)
    return constraint_dict


def reorder_columns(columns):
    """
    Reorder the columns for creating constraint, preserve primary key ordering
    ['task_id', 'dag_id', 'execution_date']
    :param columns: columns retrieved from DB related to constraint
    :return: ordered column
    """
    ordered_columns = list()
    for column in ['task_id', 'dag_id', 'execution_date']:
        if column in columns:
            ordered_columns.append(column)

    for column in columns:
        if column not in ['task_id', 'dag_id', 'execution_date']:
            ordered_columns.append(column)

    return ordered_columns


def drop_constraint(operator, constraint_dict):
    for constraint, columns in constraint_dict.items():
        if 'execution_date' in columns:
            if constraint[1].lower().startswith("primary"):
                operator.drop_constraint(
                    constraint[0],
                    type_='primary'
                )
            elif constraint[1].lower().startswith("unique"):
                operator.drop_constraint(
                    constraint[0],
                    type_='unique'
                )


def create_constraint(operator, constraint_dict):
    for constraint, columns in constraint_dict.items():
        if 'execution_date' in columns:
            if constraint[1].lower().startswith("primary"):
                operator.create_primary_key(
                    constraint_name=constraint[0],
                    columns=reorder_columns(columns)
                )
            elif constraint[1].lower().startswith("unique"):
                operator.create_unique_constraint(
                    constraint_name=constraint[0],
                    columns=reorder_columns(columns)
                )


def modify_execution_date_with_constraint(conn, batch_operator, table_name, type_, nullable):
    """
         Helper function changes type of column execution_date by
         dropping and recreating any primary/unique constraint associated with
         the column

         :param conn: sql connection object
         :param batch_operator: batch_alter_table for the table
         :param table_name: table name
         :param type_: DB column type
         :param nullable: nullable (boolean)
         :return: a dictionary of ((constraint name, constraint type), column name) of table
         :rtype: defaultdict(list)
         """
    constraint_dict = get_table_constraints(conn, table_name)
    drop_constraint(batch_operator, constraint_dict)
    batch_operator.alter_column(
        column_name="execution_date",
        type_=type_,
        nullable=nullable,
    )
    create_constraint(batch_operator, constraint_dict)
