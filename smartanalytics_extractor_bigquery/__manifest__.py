{
    'name': 'Smart Analytics - Extract BigQuery',
    'summary': 'Extract data to BigQuery',
    'version': '1.1',
    'category': 'Other',
    'author': 'Idealis Consulting',
    'website': 'https://idealisconsulting.com/',
    'depends': ['smartanalytics_extractor'],
    'external_dependencies': {'python': ['google-cloud-bigquery']},
    'data': [
        'data/ir_cron.xml',
        'views/smartanalytics_extractor.xml',
    ],
    'installable': True,
}
