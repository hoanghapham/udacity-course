help:
	@echo "Available make commands:"
	@echo "  create-emr-cluster:          Create an EMR cluster. Environment vars AWS_KEY_NAME & AWS_SUBNET_ID are needed"
	@echo "  list-active-clusters:        List clusters that are active (state: STARTING, WAITING, TERMINATING)"
	@echo "  build-etl-dependencies:      Build necessary dependencies file for the ETL job"
	@echo "  run-etl:                     Run the ETL job"
	@echo "  install-aws-cli:             Convenience command to install AWS CLI"
	@echo "  install-pyenv:               Convenience command to install pyenv"
	@echo "  prepare-environment:         Convenience command to prepare the needed environment"

create-emr-cluster:
	bash scripts/create_emr.sh

list-active-clusters:
	aws emr list-clusters --active

build-etl-dependencies:
	zip -r utils.zip utils/

run-etl:
	spark-submit --py-files utils.zip etl.py

install-aws-cli:
	mkdir -p tmp
	curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "tmp/awscliv2.zip"
	unzip -o tmp/awscliv2.zip -d tmp/ 
	sudo ./tmp/aws/install

install-pyenv:
	curl https://pyenv.run | bash

prepare-environment:
	pyenv install -f 3.7.14
	pyenv local 3.7.14
	pip install -r requirements.txt
