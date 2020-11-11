#!groovy

// import shared library: https://github.com/cmu-delphi/jenkins-shared-library
@Library('jenkins-shared-library') _

def indicator_list = ["cdc_covidnet", "claims_hosp", "combo_cases_and_deaths", "google_symptoms", "jhu", "nchs_mortality", "quidel", "quidel_covidtest", "safegraph", "safegraph_patterns", "usafacts"]
def build_and_package

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
        stage('Build and Package') {
            when {
                changeRequest target: "test-main";
            }
            steps {
                script {
                    // Do some magical thing here...
                    // for (String indicator : indicator_list) { // TEST
                    //     println ("${indicator}")
                    indicator_list.each { f ->
                        build_and_package[f] = {
                            //echo f.toString()
                            println f
                        }
                    }
                    parallel build_and_package
                }
                // sh "jenkins/${INDICATOR}-jenkins-build.sh"
                sh "echo This is a thing happening on ${BRANCH_NAME}/${CHANGE_TARGET}" // TEST
            }
        }
        stage('Deploy staging') {
            when {
                branch "main";
            }
            steps {
                sh "echo This is a thing happening on ${BRANCH_NAME}/${CHANGE_TARGET}" // TEST
                // sh "jenkins/${INDICATOR}-jenkins-test.sh"
            }
        }
        stage('Deploy production') {
            when {
                branch "prod"; // TODO Rename to new production branch
            }
            steps {
                sh "echo This is a thing happening on ${BRANCH_NAME}/${CHANGE_TARGET}" // TEST
                // sh "jenkins/${INDICATOR}-jenkins-test.sh"
            }
        }
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
