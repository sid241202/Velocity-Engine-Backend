pipeline {
    agent any
    environment {
        REGISTRY = credentials('harbor-registry-url')
        IMAGE_NAME = 'velocity-engine/control-plane-backend-go'
        VERSION = readFile('VERSION').trim()
    }
    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }
        stage('Build Binary') {
            steps {
                sh 'go mod tidy'
                sh 'CGO_ENABLED=1 go build -o server ./cmd/server'
            }
        }
        stage('Docker Build') {
            steps {
                sh "docker build -t ${REGISTRY}/${IMAGE_NAME}:${VERSION} -t ${REGISTRY}/${IMAGE_NAME}:latest ."
            }
        }
        stage('Docker Push') {
            steps {
                withCredentials([usernamePassword(credentialsId: 'harbor-credentials', usernameVariable: 'DOCKER_USER', passwordVariable: 'DOCKER_PASS')]) {
                    sh "echo ${DOCKER_PASS} | docker login ${REGISTRY} -u ${DOCKER_USER} --password-stdin"
                    sh "docker push ${REGISTRY}/${IMAGE_NAME}:${VERSION}"
                    sh "docker push ${REGISTRY}/${IMAGE_NAME}:latest"
                }
            }
        }
        stage('Update K8s Manifest') {
            steps {
                sh "sed -i 's|image:.*|image: ${REGISTRY}/${IMAGE_NAME}:${VERSION}|' k8s/deployment.yaml"
                sh "git add k8s/deployment.yaml"
                sh "git commit -m 'ci: update image to ${VERSION}' || true"
                sh "git push origin HEAD"
            }
        }
    }
    post {
        always {
            sh 'docker logout ${REGISTRY} || true'
        }
    }
}
