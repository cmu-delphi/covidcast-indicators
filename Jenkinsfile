/* import shared library */
@Library('jenkins-shared-library') _

pipeline {
    agent any

    environment {
        // Get the indicator name.
        // INDICATOR = env.BRANCH_NAME.replaceAll("deploy-", "")
        INDICATOR = env.CHANGE_BRANCH.replaceAll("deploy-", "")
    }

    stages {
        stage('Build') {
            when {
                // branch "deploy-*"
                changeRequest target: "deploy-jhu"
            }
            steps {
                sh "jenkins/${env.INDICATOR}-jenkins-build.sh"
            }
        }

        stage('Test') {
            when {
                // branch "deploy-*"
                changeRequest target: "deploy-jhu"
            }
            steps {
                sh "jenkins/${env.INDICATOR}-jenkins-test.sh"
            }
        }
        
        stage('Package') {
            when {
                // branch "deploy-*"
                changeRequest target: "deploy-jhu"
            }
            steps {
                sh "jenkins/jenkins-package.sh"
            }
        }

        stage('Deploy') {
            when {
                branch "deploy-*"
            }
            steps {
                sh "jenkins/jenkins-deploy.sh"
            }
        }
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
