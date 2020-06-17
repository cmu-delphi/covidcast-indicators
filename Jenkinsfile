// import shared library - https://github.com/cmu-delphi/jenkins-shared-library
@Library('jenkins-shared-library') _

pipeline {
    agent any

    environment {
        script {
            INDICATOR = getIndicatorName()
        // script {
        //     // Get the indicator name.
        //     if ( env.BRANCH_NAME.exists() ) {
        //         INDICATOR = env.BRANCH_NAME.replaceAll("deploy-", "")
        //     }
        //     else if ( env.CHANGE_TARGET.exists() ) {
        //         INDICATOR = env.CHANGE_TARGET.replaceAll("deploy-", "")
        //     }
        //     else {
        //         INDICATOR = ""
        //     }
        // }
        }
    }

    stages {
        // stage ("Environment") {
        //     steps {
        //         script {
        //             // Get the indicator name.
        //             INDICATOR = getIndicatorName
        //         } 
        //         sh "env"
        //         echo "${env.INDICATOR}"
        //     }
        // }

        stage('Build') {
            // when {
            //     branch "deploy-*"
            //     // changeRequest target: "deploy-jhu"
            // }
            steps {
                // sh "jenkins/${env.INDICATOR}-jenkins-build.sh"
                // sh "env" Let us level set and find out what we have to work with.
                echo "hi!"
                echo "${env.INDICATOR}"
            }
        }

    //     stage('Test') {
    //         when {
    //             // branch "deploy-*"
    //             changeRequest target: "deploy-jhu"
    //         }
    //         steps {
    //             sh "jenkins/${env.INDICATOR}-jenkins-test.sh"
    //         }
    //     }
        
    //     stage('Package') {
    //         when {
    //             // branch "deploy-*"
    //             changeRequest target: "deploy-jhu"
    //         }
    //         steps {
    //             sh "jenkins/jenkins-package.sh"
    //         }
    //     }

    //     stage('Deploy') {
    //         when {
    //             branch "deploy-*"
    //         }
    //         steps {
    //             sh "jenkins/jenkins-deploy.sh"
    //         }
    //     }
    }

    post {
        always {
            script {
                /* Use slackNotifier.groovy from shared library and provide current build result as parameter */   
                slackNotifier(currentBuild.currentResult)
                //cleanWs()
            }
        }
    }
}
