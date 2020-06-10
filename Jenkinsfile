/* import shared library */
@Library('jenkins-shared-library') _

pipeline {
    agent any

    stages {
        stage('Build') {
            when {
                branch 'bgc/deploy-test'
                // branch 'master'
            }
            steps {
                sh 'jenkins/jhu-build.sh'
            }
        }

        stage('Test') {
            when {
                branch 'bgc/deploy-test'
                // branch 'master'
            }
            steps {
                sh 'jenkins/jhu-test.sh'
            }
        }

        stage('Deploy') {
            when {
                branch 'bgc/deploy-test'
            }
            steps {
                echo 'Deploying...' // Do some work here...
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
