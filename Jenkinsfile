/* import shared library from:
   - https://github.com/cmu-delphi/jenkins-shared-library */
@Library('jenkins-shared-library') _

pipeline {

    agent any

    // environment {
    //     script {
    //         // Get the indicator name.
    //         if ( env.BRANCH_NAME ) {
    //             INDICATOR = env.BRANCH_NAME.replaceAll("deploy-", "")
    //         }
    //         else if ( env.CHANGE_TARGET ) {
    //             INDICATOR = env.CHANGE_TARGET.replaceAll("deploy-", "")
    //         }
    //         else {
    //             INDICATOR = ""
    //         }
    //     }
    // }

    stages {

        stage ("Environment") {            
            when {
                ( branch "deploy-*" || changeRequest target: "deploy-jhu" )
            }
            steps {
                script {
                    // Get the indicator name, checking for CHANGE_TARGET first.
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
                // branch "deploy-*"
                changeRequest target: "deploy-jhu"
            }
            steps {
                sh "env" // DEBUG
                sh "jenkins/${INDICATOR}-jenkins-build.sh"
            }
        }

        stage('Test') {
            when {
                // branch "deploy-*"
                changeRequest target: "deploy-jhu"
            }
            steps {
                sh "jenkins/${INDICATOR}-jenkins-test.sh"
            }
        }
        
        stage('Package') {
            when {
                // branch "deploy-*"
                changeRequest target: "deploy-jhu"
            }
            steps {
                echo "${INDICATOR}" // DEBUG
                sh "jenkins/${INDICATOR}-jenkins-package.sh"
            }
        }

        stage('Deploy') {
            when {
                branch "deploy-*"
                // changeRequest branch: "deploy-jhu"
            }
            steps {
                echo "${INDICATOR}" //DEBUG
                sh env
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
                //cleanWs()
            }
        }
    }
}
