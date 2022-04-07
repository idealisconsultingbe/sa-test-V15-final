import json
import datetime
import re

import mysql.connector
from odoo import api, fields, models, _
from odoo.exceptions import ValidationError


class SmartanalyticsExtractorBackend(models.Model):
    _inherit = 'smartanalytics.extractor.backend'

    type = fields.Selection(selection_add=[('mysql', 'MySQL')])
    mysql_host = fields.Char(string="MySQL Host")
    mysql_port = fields.Integer(string="MySQL Port", default=3306)
    mysql_user = fields.Char(string="MySQL User")
    mysql_password = fields.Char(string="MySQL Password")
    mysql_database = fields.Char(string="MySQL Database")

    def _default_python_code(self):
        res = super()._default_python_code()
        return res + """# Available variables:
#  - mysql_host
#  - mysql_port
#  - mysql_user
#  - mysql_password
#  - mysql_database
#
# Create a connection using this line of code:
# cnx = mysql.connector.connect(host=mysql_host, port=mysql_port, user=mysql_user, password=mysql_password, database=mysql_database)
# Get a cursor with this line:
# cursor = cnx.cursor()
# Don't forget to commit, close the cursor and close the connection:
# cnx.commit()
# cursor.close()
# cnx.close()
#
\n\n\n"""

    def test_connection(self):
        self.ensure_one()
        if self.type == 'mysql':
            try:
                cnx = self._get_mysql_connection()
                try:
                    cursor = cnx.cursor()
                except Exception:
                    raise ValidationError(_('Error while getting cursor from to MySQL connection'))
                else:
                    cursor.close()
            except Exception:
                raise ValidationError(_('Error while connecting to MySQL Database'))
            else:
                cnx.close()
        else:
            super().test_connection()

    def _get_mysql_connection(self):
        self.ensure_one()
        cnx = mysql.connector.connect(host=self.mysql_host, port=self.mysql_port, user=self.mysql_user, password=self.mysql_password,
                                      database=self.mysql_database)
        return cnx

    def _get_eval_context(self):
        eval_context = super()._get_eval_context()
        eval_context.update({
            'mysql_host': self.mysql_host,
            'mysql_port': self.mysql_port,
            'mysql_user': self.mysql_user,
            'mysql_password': self.mysql_password,
            'mysql_database': self.mysql_database,
        })
        return eval_context


class SmartanalyticsExtractorExtract(models.Model):
    _inherit = 'smartanalytics.extractor.extract'

    def action_run_import(self):
        res = super().action_run_import()
        for record in self:
            if record.type == 'mysql':
                try:
                    cnx = record.backend_id._get_mysql_connection()
                    cursor = cnx.cursor()
                    record._mysql_drop_table(cursor)
                    record._mysql_create_table(cursor)
                    record._mysql_insert_into_table(cursor)
                    cnx.commit()
                    record.log = 'Import finished successfully !'
                    record.state = 'succeed'
                except Exception as error:
                    errors = f'Import failed !!\n\nErrors:\n{error}'
                    record.log = errors
                    record.state = 'failed'
                else:
                    cursor.close()
                    cnx.close()
        return res

    def _mysql_drop_table(self, cursor):
        self.ensure_one()
        query = f"DROP TABLE IF EXISTS {self.table}"
        cursor.execute(query)

    def _mysql_create_table(self, cursor):
        self.ensure_one()
        fields = ', '.join(self._mysql_get_table_fields())
        query = f"CREATE TABLE {self.table} ({fields})"
        cursor.execute(query)

    def _mysql_get_table_fields(self):
        self.ensure_one()
        type_mapping = {
            'NUMERIC': 'INT',
            'BOOL': 'TINYINT',
            'STRING': 'TEXT',
        }
        fields = []
        for field in self.field_ids:
            field_type = type_mapping.get(field.dwh_type, field.dwh_type)
            field_required = 'NOT NULL' if field.dwh_required else ''
            declaration = f"{field.dwh_name} {field_type} {field_required}"
            fields.append(declaration)
        return fields

    def _mysql_insert_into_table(self, cursor):
        self.ensure_one()

        column_names = dict([(field.column, field.dwh_name) for field in self.field_ids])
        column_types = dict([(field.column, field.dwh_type) for field in self.field_ids])
        columns = self._get_columns_from_query()
        fields = ', '.join([column_names[column] for column in columns])
        placeholders = ', '.join(['%s' for f in columns])
        query = f"INSERT INTO {self.table} ({fields}) VALUES ({placeholders})"

        self.env.cr.execute(self.query)
        for row in self.env.cr.fetchall():
            values = []
            for i, column in enumerate(columns):
                value = row[i]
                if column_types.get(column) == 'BOOL':
                    values.append(1 if value else 0)
                elif column_types.get(column) == 'DATE' and isinstance(value, (datetime.date, datetime.datetime)):
                    values.append(value.strftime('%Y-%m-%d'))
                elif column_types.get(column) == 'TIME' and isinstance(value, (datetime.date, datetime.datetime)):
                    values.append(value.strftime('%H:%M:%S'))
                elif column_types.get(column) == 'DATETIME' and isinstance(value, (datetime.date, datetime.datetime)):
                    values.append(value.strftime('%Y-%m-%d  %H:%M:%S'))
                elif value is False:
                    values.append(None)
                else:
                    values.append(value)
            cursor.execute(query, values)
