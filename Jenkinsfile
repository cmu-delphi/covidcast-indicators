/* import shared library */
@Library('jenkins-shared-library') _

pipeline {
    agent any

    stages {
        stage('Build') {
            steps {
                echo 'Building...' // Do some work here...
            }
        }

        stage('Test') {
            steps {
                echo 'Testing...' // Do some work here...
            }
        }

        stage('Deploy') {
            steps {
                echo 'Deploying...' // Do some work here...
            }
        }
    }

    post {
        always {
            script {
                slackNotifier(currentBuild.currentResult)
            }
            /* Use slackNotifier.groovy from shared library and provide current build result as parameter */   
            // slackNotifier(currentBuild.currentResult)
            // cleanWs()
            // slackSend color: "good", message: "Yarssss."
        }
    }
}
