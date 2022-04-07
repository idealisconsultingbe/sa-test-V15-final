{
    'name': 'Smart Analytics - Extract MySQL',
    'summary': 'Extract data to MySQL',
    'version': '1.1',
    'category': 'Other',
    'author': 'Idealis Consulting',
    'website': 'https://idealisconsulting.com/',
    'depends': ['smartanalytics_extractor'],
    'external_dependencies': {'python': ['mysql-connector-python']},
    'data': [
        'data/ir_cron.xml',
        'views/smartanalytics_extractor.xml',
    ],
    'installable': True,
}
