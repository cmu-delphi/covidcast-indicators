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
                sh 'jenkins/jhu-jenkins-build.sh'
            }
        }

        stage('Test') {
            when {
                branch 'bgc/deploy-test'
                // branch 'master'
            }
            steps {
                sh 'jenkins/jhu-jenkins-test.sh'
            }
        }
        
        stage('Package') {
            when {
                branch 'bgc/deploy-test'
                // branch 'master'
            }
            steps {
                sh 'jenkins/jhu-jenkins-package.sh'
            }
        }

        stage('Deploy') {
            when {
                branch 'bgc/deploy-test'
            }
            steps {
                sh 'jenkins/jhu-jenkins-deploy.sh'
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
