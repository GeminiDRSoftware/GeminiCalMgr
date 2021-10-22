#!/usr/bin/env groovy
/*
 * Jenkins Pipeline for GeminiCalMgr
 *
 * by Bruno C. Quint
 * adapted for GeminiCalMgr by Oliver Oberdorf
 *
 * Required Plug-ins:
 * - CloudBees File Leak Detector
 * - Cobertura Plug-in
 * - Warnings NG
 */

pipeline {
    agent any

    options { skipDefaultCheckout() }

    environment {
        PATH = "$JENKINS_HOME/anaconda3-dev-oly/bin:$PATH"
        CONDA_ENV_FILE = ".jenkins/conda_py3env_stable.yml"
        CONDA_ENV_NAME_DEPRECATED = "py3_stable"
        CONDA_ENV_NAME = "fitsstorage_pipeline_venv"
        TEST_IMAGE_PATH = "/tmp/archive_test_images"
        TEST_IMAGE_CACHE = "/tmp/cached_archive_test_images"
    }

    stages {
        stage ("Prepare"){

            steps{
                echo 'STARTED'

//                 checkout scm

                echo 'Checking Out FitsStorageDB'
                dir('FitsStorageDB') {
                    git url: 'git@gitlab.gemini.edu:DRSoftware/FitsStorageDB.git',
                    branch: 'release/1.0.x',
                    credentialsId: '23171fd7-22a8-459a-bbf3-ec2e65ec56b7'
                }

                echo 'Checking Out GeminiCalMgr'
                dir('GeminiCalMgr') {
                    git url: 'git@gitlab.gemini.edu:DRSoftware/GeminiCalMgr.git',
                    branch: 'release/1.1.x',
                    credentialsId: '23171fd7-22a8-459a-bbf3-ec2e65ec56b7'
                }
            }

        }

        stage('Building Docker Containers') {
            steps {
                script {
                    def geminicalmgrimage = docker.build("gemini/geminicalmgr:jenkins", " -f GeminiCalMgr/docker/geminicalmgr-jenkins/Dockerfile .")
                    sh '''
                    echo "Clear existing Docker infrastructure to start with a blank slate"
                    docker network create geminicalmgr-jenkins || true
                    docker container rm geminicalmgr-jenkins || true
                    '''
                    def postgres = docker.image('postgres:12').withRun(" --network geminicalmgr-jenkins --name geminicaldb-jenkins -e POSTGRES_USER=fitsdata -e POSTGRES_PASSWORD=fitsdata -e POSTGRES_DB=fitsdata") { c ->
                        try {
                            docker.image('gemini/geminicalmgr:jenkins').inside(" -v reports:/data/reports -v /data/pytest_tmp:/tmp  --network geminicalmgr-jenkins -e USE_AS_ARCHIVE=False -e STORAGE_ROOT=/tmp/jenkins_pytest/dataflow -e FITS_DB_SERVER=\"fitsdata:fitsdata@geminicaldb-jenkins\" -e PYTEST_SERVER=http://archive-jenkins -e TEST_IMAGE_PATH=/tmp/archive_test_images -e TEST_IMAGE_CACHE=/tmp/cached_archive_test_images -e BLOCKED_URLS=\"\" -e CREATE_TEST_DB=False -e PYTHONPATH=/opt/FitsStorage:/opt/DRAGONS:/opt/FitsStorageDB:/opt/GeminiCalMgr") {
                                sh 'python3 /opt/FitsStorageDB/gemini_obs_db/scripts/create_tables.py'
                                echo "Running tests against docker containers"
                                sh  '''
                                    mkdir -p /tmp/archive_test_images
                                    mkdir -p /tmp/cached_archive_test_images
                                    coverage run --omit "/usr/lib/*,/usr/local/*,/opt/DRAGONS/*,/opt/FitsStorageDB/*" -m pytest /opt/GeminiCalMgr/tests
                                    coverage report -m --fail-under=1
                                    '''
                            }
                        } catch (exc) {
                            sh "docker logs geminicalmgr-jenkins"
                            throw exc
                        }
                    }
                }
            }
        }
    }
    post {
        always {
          junit (
            allowEmptyResults: true,
            testResults: 'reports/*_results.xml'
            )
          sh '''
             if [ -f dragons-repo.txt ]; then rm -rf `cat dragons-repo.txt`; fi
             docker rmi gemini/geminicalmgr:jenkins || true
             docker network rm geminicalmgr-jenkins || true
          '''
        }
        success {
            echo 'SUCCESSFUL'
        }
        failure {
            echo 'FAILED'
        }
    }
}
