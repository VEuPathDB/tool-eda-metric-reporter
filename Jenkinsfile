#!groovy


properties([
  parameters([
    stringParam(name: 'ENV', defaultValue: 'dev', description: 'Environment to target when scraping report data'),
    stringParam(name: 'EDA_URL', defaultValue: 'https://edadata-dev.local.apidb.org:8443', description: 'EDA site to use for user metrics service URL'),
    stringParam(name: 'PROM_URL', defaultValue: 'sloe.uga.apidb.org:9090', description: 'URL to use for prometheus metrics'),
    stringParam(name: 'CALENDAR_MONTH', defaultValue: 'auto', description: 'Calendar month to use when querying metrics in yyyy-MM'),
    stringParam(name: 'NODE', defaultValue: 'palm', description: 'Node to run on'),
    stringParam(name: 'DB_LDAP_CN', defaultValue: 'cecommdevn', description: 'Target database'),
    stringParam(name: 'LDAP_HOST', defaultValue: '', description: 'LDAP host'),
    stringParam(name: 'LDAP_QUERY', defaultValue: '', description: 'LDAP query'),
    stringParam(name: 'DB_USER', defaultValue: '', description: 'DB user'),
    stringParam(name: 'SECRETS_FILE', defaultValue: '', description: 'Secrets file'),
    stringParam(name: 'PROJECT_ID', defaultValue: 'ClinEpiDB', description: 'Project ID for which to collect metrics')
  ])
])

node(params.NODE) {
  checkout scm

  sh 'python3 -m venv pyenv'                   // Create a virtual environment in the Jenkins workspace.
  Python("-m pip install --upgrade .")         // Install our usage metrics module.
  Python("bin/run.py ${params.ENV} ${params.EDA_URL} ${params.PROM_URL} ${params.CALENDAR_MONTH} ${params.DB_LDAP_CN} ${params.LDAP_HOST} ${params.LDAP_QUERY} ${params.DB_USER} ${params.SECRETS_FILE} ${params.PROJECT_ID}")
}

// Jenkins spawns a new shell each time "sh" is called. Using this command ensures we use the virtual environment each time we
// execute a python command.
def Python(String command) {
    sh script:"source ${WORKSPACE}/pyenv/bin/activate && python3 ${command}", label: "python3 ${command}"
}
