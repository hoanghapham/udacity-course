aws emr create-cluster \
    --name udacity-20221208 \
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
    --ec2-attributes KeyName=udacity-spark-asus,SubnetId=subnet-08974a26617d9eb1e \
    --instance-type m5.xlarge \
    --auto-termination-policy IdleTimeout=2000