pipeline {
    agent {
        node {
            label 'docker'
        }
    }

    environment {
        IMAGE_TAG = readMavenPom().getVersion()
        REGISTRY_URL = "https://io-docker.artifactory.swisscom.com"
        REGISTRY_CREDENTIALS_ID = "artifactory"
        PROXY_SETTINGS = "-Dhttp.proxyHost=server-proxy.corproot.net -Dhttp.proxyPort=8080 -Dhttps.proxyHost=server-proxy.corproot.net -Dhttps.proxyPort=8080 -Dhttp.nonProxyHosts=*.swisscom.com -Dhttps.nonProxyHosts=*.swisscom.com"
    }

    stages {

        stage('Build jars') {
            agent {
                docker {
                    reuseNode true
                    image 'io-docker.artifactory.swisscom.com/maven:3.6.0-jdbc-connector'
                    registryUrl env.REGISTRY_URL
                    registryCredentialsId env.REGISTRY_CREDENTIALS_ID
                }
            }
            steps {
                withMaven(maven: 'Maven', mavenLocalRepo: '.repository', globalMavenSettingsConfig: "maven-settings") {
                    sh 'export PATH=$MVN_CMD_DIR:$PATH && mvn clean package -U $PROXY_SETTINGS'
                }
            }
        }

        stage('Deploy jars') {
            agent {
                docker {
                    reuseNode true
                    image 'io-docker.artifactory.swisscom.com/maven:3.6.0-jdbc-connector'
                    registryUrl env.REGISTRY_URL
                    registryCredentialsId env.REGISTRY_CREDENTIALS_ID
                }
            }
            steps {
                withMaven(maven: 'Maven', mavenLocalRepo: '.repository', globalMavenSettingsConfig: "maven-settings") {
                    sh 'export PATH=$MVN_CMD_DIR:$PATH && mvn deploy $PROXY_SETTINGS'
                }
            }
        }
    }

    post {
        failure {
            emailext(
                    subject: '${DEFAULT_SUBJECT}',
                    recipientProviders: [[$class: 'CulpritsRecipientProvider'], [$class: 'RequesterRecipientProvider']],
                    body: '${DEFAULT_CONTENT}'
            )
        }
    }
}