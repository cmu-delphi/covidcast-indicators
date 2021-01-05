#!groovy

// Import shared lib.
@Library('jenkins-shared-library') _

/*
   Declare variables.
   - indicator_list should contain all the indicators to handle in the pipeline.
   - Keep in sync with '.github/workflows/python-ci.yml'.
   - TODO: #527 Get this list automatically from python-ci.yml at runtime.
 */
def indicator_list = ["cdc_covidnet", "changehc", "claims_hosp", "combo_cases_and_deaths", "google_health", "google_symptoms", "hhs_hosp", "jhu", "nchs_mortality", "quidel", "quidel_covidtest", "safegraph", "safegraph_patterns", "sir_complainsalot", "usafacts"]
def build_package = [:]
def deploy_staging = [:]
def deploy_production = [:]

pipeline {
    agent any
    stages {
        stage('Build and Package') {
            when {
                branch "main";
            }
            steps {
                script {
                    indicator_list.each { indicator ->
                        build_package[indicator] = {
                            sh "jenkins/build-and-package.sh ${indicator}"
                        }
                    }
                    parallel build_package
                }
            }
        }
        stage('Deploy staging') {
            when {
                branch "main";
            }
            steps {
                script {
                    indicator_list.each { indicator ->
                        deploy_staging[indicator] = {
                            sh "jenkins/deploy-staging.sh ${indicator}"
                        }
                    }
                    parallel deploy_staging
                }
                sh "jenkins/deploy-staging-api-match-list.sh"
            }
        }
        stage('Deploy production') {
            when {
                branch "prod";
            }
            steps {
                script {
                    indicator_list.each { indicator ->
                        deploy_production[indicator] = {
                            sh "jenkins/deploy-production.sh ${indicator}"
                        }
                    }
                    parallel deploy_production
                }
            }
        }
    }
    post {
        always {
            script {
                // Use slackNotifier.groovy from shared lib.
                slackNotifier(currentBuild.currentResult)
            }
        }
    }
}
