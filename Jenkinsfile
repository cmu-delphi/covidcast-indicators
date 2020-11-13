#!groovy

// Import shared lib.
@Library('jenkins-shared-library') _

// Vars.
def indicator_list = ["cdc_covidnet", "claims_hosp", "combo_cases_and_deaths", "google_symptoms", "jhu", "nchs_mortality", "quidel", "quidel_covidtest", "safegraph", "safegraph_patterns", "usafacts"]
def build_package = [:]
def deploy_staging = [:]
def deploy_production = [:]

pipeline {
    agent any
    stages {
        stage ("Environment") {            
            when {
                anyOf {
                    branch "test-main"; // TODO Switch to main when done.
                    branch "prod"; // TODO Rename to new production branch when it exists
                    changeRequest target: "test-main"; // TODO Switch to main when done.
                }
            }
            steps {
                sh "echo noop noop"
            }
        }
        stage('Build and Package') {
            when {
                changeRequest target: "test-main";
            }
            steps {
                script {
                    indicator_list.each { indicator ->
                        build_package[indicator] = {
                            sh "jenkins/jenkins-build-and-package.sh ${indicator}"
                        }
                    }
                    parallel build_package
                }
            }
        }
        stage('Deploy staging') {
            when {
                branch "test-main";
            }
            steps {
                script {
                    indicator_list.each { indicator ->
                        deploy_staging[indicator] = {
                            sh "jenkins/jenkins-deploy-staging.sh ${indicator}"
                        }
                    }
                    parallel deploy_staging
                }
            }
        }
        stage('Deploy production') {
            when {
                branch "prod"; // TODO Rename to new production branch when it exists
            }
            steps {
                script {
                    indicator_list.each { indicator ->
                        deploy_production[indicator] = {
                            sh "jenkins/jenkins-deploy-production.sh ${indicator}"
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
                //Use slackNotifier.groovy from shared lib.
                slackNotifier(currentBuild.currentResult)
            }
        }
    }
}
