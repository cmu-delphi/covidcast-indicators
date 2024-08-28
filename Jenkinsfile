#!groovy

// Import shared lib.
@Library('jenkins-shared-library') _

/*
   Declare variables.
   - indicator_list should contain all the indicators to handle in the pipeline.
   - Keep in sync with '.github/workflows/python-ci.yml'.
   - TODO: #527 Get this list automatically from python-ci.yml at runtime.
 */

def indicator_list = ['backfill_corrections', 'changehc', 'claims_hosp', 'google_symptoms', 'hhs_hosp', 'nchs_mortality', 'quidel_covidtest', 'sir_complainsalot', 'doctor_visits', 'nssp']
def build_package_main = [:]
def build_package_prod = [:]
def deploy_staging = [:]
def deploy_production = [:]

pipeline {
    agent any
    stages {
        stage('Build dev/feature branch') {
            when  {
                not {
                    anyOf {
                        branch 'main'
                        branch 'prod'
                    }
                }
            }
            steps {
                script {
                    indicator_list.each { indicator ->
                        stage("Build ${indicator}") {
                            sh "jenkins/build-indicator.sh ${indicator}"
                        }
                    }
                }
            }
        }
        stage('Build and Package main branch') {
            when {
                branch 'main'
            }
            steps {
                script {
                    indicator_list.each { indicator ->
                        stage("Build ${indicator}") {
                            sh "jenkins/build-indicator.sh ${indicator}"
                        }
                        stage("Package ${indicator}") {
                            sh "jenkins/package-indicator.sh ${indicator} main"
                        }
                    }
                }
            }
        }
        stage('Build and Package prod branch') {
            when {
                branch 'prod'
            }
            steps {
                script {
                    indicator_list.each { indicator ->
                        stage("Build ${indicator}") {
                            sh "jenkins/build-indicator.sh ${indicator}"
                        }
                        stage("Package ${indicator}") {
                            sh "jenkins/package-indicator.sh ${indicator} prod"
                        }
                    }
                }
            }
        }
        stage('Deploy main branch to staging env') {
            when {
                branch 'main'
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
            }
        }
        stage('Deploy prod branch to production env') {
            when {
                branch 'prod'
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
