#!groovy

// Import shared lib.
@Library('jenkins-shared-library') _

/*
   Declare variables.
   - indicator_list should contain all the indicators to handle in the pipeline.
   - Keep in sync with '.github/workflows/python-ci.yml'.
   - TODO: #527 Get this list automatically from python-ci.yml at runtime.
 */

//def indicator_list = ["backfill_corrections", "changehc", "claims_hosp", "google_symptoms", "hhs_hosp", "jhu", "nchs_mortality", "quidel_covidtest", "sir_complainsalot", "dsew_community_profile", "doctor_visits"]
//def indicator_list = ["backfill_corrections", "changehc"]
def indicator_list = ["changehc","backfill_corrections"]
def build_package_main = [:]
def build_package_prod = [:]
def deploy_staging = [:]
def deploy_production = [:]

pipeline {
    agent any
    stages {
        stage('Build and Package main') {
            when {
                branch "test-indicator-build";
            }
            steps {
                script {
                    indicator_list.each { indicator ->
                        build_package_main[indicator] = {
                            sh "jenkins/build-and-package.sh ${indicator} main"
                        }
                    }
                    parallel build_package_main
                }
            }
        }
        stage('Build and Package prod') {
            when {
                branch "prod";
            }
            steps {
                script {
                    indicator_list.each { indicator ->
                        build_package_prod[indicator] = {
                            sh "jenkins/build-and-package.sh ${indicator} prod"
                        }
                    }
                    parallel build_package_prod
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
