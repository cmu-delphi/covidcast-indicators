#!groovy

// Import shared lib.
@Library('jenkins-shared-library') _

def indicator_list = ["changehc", "claims_hosp", "facebook", "google_symptoms", "hhs_hosp", "jhu", "nchs_mortality", "quidel", "quidel_covidtest", "safegraph_patterns", "sir_complainsalot", "usafacts", "dsew_community_profile"]

pipeline {
    agent any
    stages {
        stage('Build and Package [Manual]') {
            steps {
                script {
                    sh "echo ${indicator_list}"
                    sh "echo test"
                }
            }
        }
    }
}
