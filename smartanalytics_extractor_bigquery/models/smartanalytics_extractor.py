import json

from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import BadRequest
from odoo import api, fields, models, _
from odoo.exceptions import ValidationError


class SmartanalyticsExtractorBackend(models.Model):
    _inherit = 'smartanalytics.extractor.backend'

    bq_project = fields.Char(string='Project')
    bq_credentials = fields.Text(string='Credentials')
    type = fields.Selection(selection_add=[('bigquery', 'BigQuery')])

    def _default_python_code(self):
        res = super()._default_python_code()
        return res + """# Available variables:
#  - bq_credentials_json: The JSON with the credentials to access the API of BigQuery
#  - bq_credentials: The credentials object for Google Cloud (google.oauth2.service_account.Credentials)
#  - bq_project_id: The id of the project in Google Cloud (ex: idealis-consulting-reporting)
#  - bq_client: The client to Google Cloud BigQuery (google.cloud.bigquery.Client)
\n\n\n"""

    def test_connection(self):
        self.ensure_one()
        if self.type == 'bigquery':
            try:
                client = self._get_bq_client()
                client.list_datasets()
                client.close()
            except Exception:
                raise ValidationError(_('Error while connecting to Google BigQuery'))
        else:
            super().test_connection()

    def _get_bq_client(self):
        self.ensure_one()
        credentials = service_account.Credentials.from_service_account_info(json.loads(self.bq_credentials))
        client = bigquery.Client(project=self.bq_project, credentials=credentials)
        return client

    def _get_eval_context(self):
        eval_context = super()._get_eval_context()
        credentials = service_account.Credentials.from_service_account_info(json.loads(self.bq_credentials))
        eval_context.update({
            'bq_credentials_json': self.bq_credentials,
            'bq_credentials': credentials,
            'bq_project_id': self.bq_project,
            'bq_client': self._get_bq_client(),
        })
        return eval_context


class SmartanalyticsExtractorExtract(models.Model):
    _inherit = 'smartanalytics.extractor.extract'

    dataset = fields.Char(string='Bigquery dataset')
    dataset_location = fields.Selection(
        selection=[('EU', 'EU'), ('US', 'US')], string='Bigquery dataset location', default='EU'
    )

    def action_run_import(self):
        res = super().action_run_import()
        for record in self:
            if self.type == 'bigquery':
                client = record.backend_id._get_bq_client()
                record._bq_create_dataset_table(client)
                record._bq_import_datas(client)
                client.close()
        return res

    def _bq_create_dataset_table(self, client=False):
        self.ensure_one()
        # Create a client, if not given in params
        auto_close = False
        if not client:
            auto_close = True
            client = self.backend_id._get_bq_client()
        self._bq_create_dataset(client)
        self._bq_create_empty_table(client)
        # Close the client, if not given in params
        if auto_close:
            client.close()

    def _bq_create_dataset(self, client):
        self.ensure_one()
        dataset_name = self._bq_get_dataset_name(client)
        dataset = bigquery.Dataset(dataset_name)
        dataset.location = self.dataset_location
        return client.create_dataset(dataset, exists_ok=True)

    def _bq_create_table(self, client):
        self.ensure_one()
        table_name = self._bq_get_table_name(client)
        schema = self._bq_make_schema()
        table = bigquery.Table(table_name, schema=schema)
        return client.create_table(table, exists_ok=True)

    def _bq_delete_table(self, client):
        self.ensure_one()
        table_name = self._bq_get_table_name(client)
        client.delete_table(table_name)

    def _bq_create_empty_table(self, client):
        self.ensure_one()
        # dataset_name = self._bq_get_dataset_name(client)
        # tables = client.list_tables(dataset_name)
        # if self.table in [t.table_id for t in tables]:
        #     self._bq_delete_table(client)
        table = self._bq_create_table(client)
        return table

    def _bq_make_schema(self):
        self.ensure_one()
        result = []
        for field in self.field_ids:
            mode = 'REQUIRED' if field.dwh_required else 'NULLABLE'
            field_type = 'INT64' if field.dwh_type == 'INT' else field.dwh_type
            bq_field = bigquery.SchemaField(field.dwh_name, field_type, mode=mode)
            result.append(bq_field)
        return result

    def _bq_get_dataset_name(self, client):
        self.ensure_one()
        return '%s.%s' % (client.project, self.dataset)

    def _bq_get_table_name(self, client):
        self.ensure_one()
        dataset_name = self._bq_get_dataset_name(client)
        return '%s.%s' % (dataset_name, self.table)

    def _bq_import_datas(self, client=False):
        self.ensure_one()
        # Create a client, if not given in params
        auto_close = False
        if not client:
            auto_close = True
            client = self.backend_id._get_bq_client()
        table_name = self._bq_get_table_name(client)
        table = client.get_table(table_name)
        rows_to_insert = self._prepare_dwh_datas()
        schema = self._bq_make_schema()
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=False,
        )
        job = client.load_table_from_json(rows_to_insert, table, location=self.dataset_location, job_config=job_config)
        try:
            job.result()
            self.log = 'Import finished successfully !'
            self.state = 'succeed'
        except BadRequest:
            errors = 'Import failed !!\n\nErrors:\n'
            for error in job.errors:
                errors += '{}\n'.format(error['message'])
            self.log = errors + '\n\n' + str(schema)
            self.state = 'failed'
        # Close the client, if not given in params
        if auto_close:
            client.close()
