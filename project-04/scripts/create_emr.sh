aws emr create-cluster \
    --name emr-$(date +%s) \
    --release-label emr-5.36.0  \
    --use-default-roles \
    --instance-count 3 \
    --applications \
        Name=Spark \
        Name=Zeppelin \
        Name=Hadoop \
        Name=Livy \
        Name=JupyterHub \
        Name=JupyterEnterpriseGateway \
        Name=Ganglia \
    --ec2-attributes KeyName=$(AWS_KEY_NAME),SubnetId=$(AWS_SUBNET_ID) \
    --instance-type m5.xlarge \
    --auto-termination-policy IdleTimeout=2000