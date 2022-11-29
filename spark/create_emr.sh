aws emr create-cluster \
    --name udacity \
    --release-label emr-5.36.0  \
    --use-default-roles \
    --instance-count 3 \
    --applications Name=Spark Name=Zeppelin Name=Hadoop Name=Livy Name=JupyterHub Name=JupyterEnterpriseGateway \
    --bootstrap-actions Path="s3://hapham/bootstrap_emr.sh" \
    --ec2-attributes KeyName=udacity-spark-2,SubnetId=subnet-08974a26617d9eb1e \
    --instance-type m5.xlarge \
    --auto-termination-policy IdleTimeout=900