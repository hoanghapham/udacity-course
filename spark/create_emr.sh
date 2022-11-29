aws emr create-cluster \
--name "udacity" \
--release-label emr-5.36.0 \
--applications Name=Spark Name=Hadoop Name=Livy Name=JupyterHub Name=JupyterEnterpriseGateway \
--ec2-attributes KeyName=udacity-spark \
--instance-type m5.xlarge \
--instance-count 3 \
--use-default-roles	