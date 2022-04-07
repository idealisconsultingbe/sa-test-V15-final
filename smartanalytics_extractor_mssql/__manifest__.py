{
    'name': 'Smart Analytics - Extract MsSQL',
    'summary': 'Extract data to MsSQL',
    'version': '1.1',
    'category': 'Other',
    'author': 'Idealis Consulting',
    'website': 'https://idealisconsulting.com/',
    'depends': ['smartanalytics_extractor'],
    'external_dependencies': {'python': ['pymssql']},
    'data': [
        'data/ir_cron.xml',
        'views/smartanalytics_extractor.xml',
    ],
    'installable': True,
}
