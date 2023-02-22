#!groovy


properties([
  parameters([
    stringParam(name: 'ENV', defaultValue: 'dev', description: 'Environment to target when scraping report data'),
    stringParam(name: 'EDA_URL', defaultValue: 'clinepidb.org', description: 'EDA site to use for user metrics service URL'),
    stringParam(name: 'PROM_URL', defaultValue: 'sloe.uga.apidb.org:9090', description: 'URL to use for prometheus metrics'),
    stringParam(name: 'CALENDAR_MONTH', defaultValue: 'auto', description: 'Calendar month to use when querying metrics in yyyy-MM')
  ])
])

node("watermelon") {
  checkout scm

  sh 'python3 -m venv pyenv'         // Create a virtual environment in the Jenkins workspace.
  Python("-m pip install distutils") // Install distutils, so we can install our module.
  Python("-m pip install .")         // Install our usage metrics module.
  Python("bin/run.py ${params.ENV} ${params.EDA_URL} ${params.PROM_URL} ${params.CALENDAR_MONTH}")
}

// Jenkins spawns a new shell each time "sh" is called. Using this command ensures we use the virtual environment each time we
// execute a python command.
def Python(String command) {
    sh script:"source ${WORKSPACE}/pyenv/bin/activate && python3 ${command}", label: "python3 ${command}"
}
