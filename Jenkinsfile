#!groovy

// import shared library: https://github.com/cmu-delphi/jenkins-shared-library
@Library('jenkins-shared-library') _

pipeline {

    agent any

    stages {

        stage ("Environment") {            
            when {
                anyOf {
                    branch "test-main"; // TODO switch to main when done.
                    branch "prod";
                    changeRequest target: "test-main"; // TODO switch to main when done.
                }
            }
            steps {
                // script {
                //     // Get a list of indicators and read them into a list.
                //     if ( env.CHANGE_TARGET ) {
                //         INDICATOR = env.CHANGE_TARGET.replaceAll("deploy-", "")
                //     }
                //     else if ( env.BRANCH_NAME ) {
                //         INDICATOR = env.BRANCH_NAME.replaceAll("deploy-", "")
                //     }
                //     else {
                //         INDICATOR = ""
                //     }
                // } 
                sh "echo This is a thing happening on ${BRANCH_NAME}/${CHANGE_TARGET}" // TEST
            }
        }

    //     stage('Build') {
    //         when {
    //             changeRequest target: "deploy-*", comparator: "GLOB"
    //         }
    //         steps {
    //             sh "jenkins/${INDICATOR}-jenkins-build.sh"
    //         }
    //     }

    //     stage('Test') {
    //         when {
    //             changeRequest target: "deploy-*", comparator: "GLOB"
    //         }
    //         steps {
    //             sh "jenkins/${INDICATOR}-jenkins-test.sh"
    //         }
    //     }
        
    //     stage('Package') {
    //         when {
    //             changeRequest target: "deploy-*", comparator: "GLOB"
    //         }
    //         steps {
    //             sh "jenkins/${INDICATOR}-jenkins-package.sh"
    //         }
    //     }

    //     stage('Deploy to staging env') {
    //         when {
    //             changeRequest target: "deploy-*", comparator: "GLOB"
    //         }
    //         steps {
    //             sh "jenkins/jenkins-deploy-staging.sh ${INDICATOR}"
    //         }
    //     }

    //     stage('Deploy') {
    //         when {
    //             branch "deploy-*"
    //         }
    //         steps {
    //             sh "jenkins/${INDICATOR}-jenkins-deploy.sh"
    //         }
    //     }
    }

    post {
        always {
            script {
                /*
                Use slackNotifier.groovy from shared library and provide current
                build result as parameter.
                */
                slackNotifier(currentBuild.currentResult)
            }
        }
    }
}

// TODO: Purge these when done testing
// TEST1
