#!groovy

// import shared library: https://github.com/cmu-delphi/jenkins-shared-library
@Library('jenkins-shared-library') _

pipeline {

    agent any

    stages {

        stage ("Environment") {            
            when {
                anyOf {
                    branch "deploy-*";
                    changeRequest target: "deploy-*", comparator: "GLOB"
                }
            }
            steps {
                script {
                    // Get the indicator name from the pipeline env.
                    if ( env.CHANGE_TARGET ) {
                        INDICATOR = env.CHANGE_TARGET.replaceAll("deploy-", "")
                    }
                    else if ( env.BRANCH_NAME ) {
                        INDICATOR = env.BRANCH_NAME.replaceAll("deploy-", "")
                    }
                    else {
                        INDICATOR = ""
                    }
                } 
            }
        }

        stage('Build') {
            when {
                changeRequest target: "deploy-*", comparator: "GLOB"
            }
            steps {
                sh "jenkins/${INDICATOR}-jenkins-build.sh"
            }
        }

        stage('Test') {
            when {
                changeRequest target: "deploy-*", comparator: "GLOB"
            }
            steps {
                sh "jenkins/${INDICATOR}-jenkins-test.sh"
            }
        }
        
        stage('Package') {
            when {
                changeRequest target: "deploy-*", comparator: "GLOB"
            }
            steps {
                sh "jenkins/${INDICATOR}-jenkins-package.sh"
            }
        }

        stage('Deploy') {
            when {
                branch "deploy-*"
            }
            steps {
                sh "jenkins/${INDICATOR}-jenkins-deploy.sh"
            }
        }
    }

    post {
        always {
            script {
                /*
                Use slackNotifier.groovy from shared library and provide current
                build result as parameter
                */   
                slackNotifier(currentBuild.currentResult)
            }
        }
    }
}
