package aws

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kafkaconnect"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func resourceAwsMskConnector() *schema.Resource {
	return &schema.Resource{
		CreateContext: resourceAwsMskConnectCreate,
		ReadContext:   resourceAwsMskConnectorRead,
		UpdateContext: resourceAwsMskConnectorUpdate,
		DeleteContext: resourceAwsMskConnectorDelete,

		Schema: map[string]*schema.Schema{
			"connector_name": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
				Computed: false,
			},
			"connector_description": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: false,
				Computed: false,
			},
			"mcu_count": {
				Type:     schema.TypeInt,
				Required: true,
				ForceNew: false,
				Computed: false,
			},
			"workers_count": {
				Type:     schema.TypeInt,
				Required: true,
				ForceNew: false,
				Computed: false,
			},
			"auth_type": {
				Type:     schema.TypeString,
				Required: false,
				Default:  kafkaconnect.KafkaClusterClientAuthenticationTypeIam,
				ForceNew: false,
				Computed: false,
			},
			"encryption_type": {
				Type:     schema.TypeString,
				Required: false,
				Default:  kafkaconnect.KafkaClusterEncryptionInTransitTypeTls,
				ForceNew: false,
				Computed: false,
			},
			"bootstrap_servers": {
				Type:     schema.TypeSet,
				Required: true,
				ForceNew: false,
				Computed: false,
			},
			"security_groups": {
				Type:     schema.TypeSet,
				Required: true,
				ForceNew: false,
				Computed: false,
			},
			"subnets": {
				Type:     schema.TypeSet,
				Required: true,
				ForceNew: false,
				Computed: false,
			},
			"kafka_connect_version": {
				Type:     schema.TypeString,
				Required: false,
				Default:  "TODO set current version",
				ForceNew: true,
				Computed: false,
			},
			"cw_log_group": {
				Type:     schema.TypeString,
				Required: false,
				ForceNew: false,
				Computed: false,
			},
			"firehose_log_delivery_stream": {
				Type:     schema.TypeString,
				Required: false,
				ForceNew: false,
				Computed: false,
			},
			"s3_log_bucket": {
				Type:     schema.TypeString,
				Required: false,
				ForceNew: false,
				Computed: false,
			},
			"s3_log_prefix": {
				Type:     schema.TypeString,
				Required: false,
				ForceNew: false,
				Computed: false,
			},
			"execution_role_arn": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: false,
				Computed: false,
			},
			"plugins_arns": {
				Type:     schema.TypeSet,
				Required: false,
				ForceNew: false,
				Computed: false,
			},
		},
	}
}

func resourceAwsMskConnectCreate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	conn := meta.(*AWSClient).kafkaconnectconn

	input, err := newCreateConnectorRequest(conn, d)
	if err != nil {
		return diag.FromErr(err)
	}

	output, err := conn.CreateConnector(input)

	if err != nil {
		return diag.FromErr(fmt.Errorf("error creating MSK Connector: %s", err))
	}

	d.SetId(aws.StringValue(output.ConnectorArn))

	return resourceAwsMskConnectorRead(ctx, d, meta)
}

func resourceAwsMskConnectorRead(_ context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	conn := meta.(*AWSClient).kafkaconnectconn

	input := &kafkaconnect.DescribeConnectorInput{
		ConnectorArn: aws.String(d.Id()),
	}

	connectorInput := input
	c, err := conn.DescribeConnector(connectorInput)

	if err != nil {
		d.SetId("")
		return diag.FromErr(err)
	}
	d.SetId(*c.ConnectorArn)

	var diagnostics diag.Diagnostics
	fields := map[string]interface{}{
		"connector_name":               c.ConnectorName,
		"connector_description":        c.ConnectorDescription,
		"mcu_count":                    c.Capacity.ProvisionedCapacity.McuCount,
		"workers_count":                c.Capacity.ProvisionedCapacity.WorkerCount,
		"auth_type":                    c.KafkaClusterClientAuthentication.AuthenticationType,
		"encryption_type":              c.KafkaClusterEncryptionInTransit.EncryptionType,
		"bootstrap_servers":            c.KafkaCluster.ApacheKafkaCluster.BootstrapServers,
		"security_groups":              c.KafkaCluster.ApacheKafkaCluster.Vpc.SecurityGroups,
		"subnets":                      c.KafkaCluster.ApacheKafkaCluster.Vpc.Subnets,
		"kafka_connect_version":        c.KafkaConnectVersion,
		"cw_log_group":                 c.LogDelivery.WorkerLogDelivery.CloudWatchLogs.LogGroup,
		"firehose_log_delivery_stream": c.LogDelivery.WorkerLogDelivery.Firehose.DeliveryStream,
		"s3_log_bucket":                c.LogDelivery.WorkerLogDelivery.S3.Bucket,
		"s3_log_prefix":                c.LogDelivery.WorkerLogDelivery.S3.Prefix,
		"execution_role_arn":           c.ServiceExecutionRoleArn,
		"plugins_arns":                 c.ServiceExecutionRoleArn,
	}
	for k, v := range fields {
		err = d.Set(k, v)
		if err != nil {
			fromErr := diag.FromErr(err)
			for _, d := range fromErr {
				diagnostics = append(diagnostics, d)
			}
		}
	}

	return diagnostics
}

func resourceAwsMskConnectorUpdate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	conn := meta.(*AWSClient).kafkaconnectconn
	connectorArn := aws.String(d.Id())

	currentPlugin, err := conn.DescribeConnector(&kafkaconnect.DescribeConnectorInput{
		ConnectorArn: connectorArn,
	})
	if err != nil {
		return diag.FromErr(err)
	}

	mcuCount := d.Get("mcu_count").(int64)
	workerCount := d.Get("worker_count").(int64)
	input := &kafkaconnect.UpdateConnectorInput{
		ConnectorArn:   aws.String(d.Id()),
		CurrentVersion: currentPlugin.CurrentVersion,
		Capacity: &kafkaconnect.CapacityUpdate{
			ProvisionedCapacity: &kafkaconnect.ProvisionedCapacityUpdate{
				McuCount:    &mcuCount,
				WorkerCount: &workerCount,
			},
		},
	}

	_, err = conn.UpdateConnector(input)
	if err != nil {
		return diag.FromErr(err)
	}

	return resourceAwsMskConnectorRead(ctx, d, meta)
}

func resourceAwsMskConnectorDelete(_ context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	conn := meta.(*AWSClient).kafkaconnectconn

	connectorArn := aws.String(d.Id())

	connector, err := conn.DescribeConnector(&kafkaconnect.DescribeConnectorInput{
		ConnectorArn: connectorArn,
	})
	if err != nil {
		return diag.FromErr(err)
	}

	_, err = conn.DeleteConnector(&kafkaconnect.DeleteConnectorInput{
		ConnectorArn:   connectorArn,
		CurrentVersion: connector.CurrentVersion,
	})
	if err != nil {
		return diag.FromErr(err)
	}

	return nil
}

func newCreateConnectorRequest(conn *kafkaconnect.KafkaConnect, d *schema.ResourceData) (*kafkaconnect.CreateConnectorInput, error) {
	connectorName := d.Get("connector_name").(string)
	connectorDescription := d.Get("connector_description").(string)

	connectorConfiguration := d.Get("connector_configuration").(map[string]*string)

	mcuCount := d.Get("mcu_count").(int64)
	workerCount := d.Get("worker_count").(int64)

	authType := d.Get("auth_type").(string)
	encryptionType := d.Get("encryption_type").(string)

	bootstrapServers := d.Get("bootstrap_servers").(string)
	securityGroups := d.Get("security_groups").([]*string)
	subnets := d.Get("subnets").([]*string)

	kafkaConnectVersion := d.Get("kafka_connect_version").(string)

	roleArn := d.Get("execution_role_arn").(*string)

	var workerConfiguration *kafkaconnect.WorkerConfiguration

	pluginArns := d.Get("plugins_arns").([]string)
	plugins, err := loadCustomPlugins(conn, pluginArns)
	if err != nil {
		return nil, err
	}

	enabled := true
	cwLogGroup := d.Get("cw_log_group").(*string)
	var cloudWatchLogs *kafkaconnect.CloudWatchLogsLogDelivery
	if &cwLogGroup != nil {
		cloudWatchLogs = &kafkaconnect.CloudWatchLogsLogDelivery{
			Enabled:  &enabled,
			LogGroup: cwLogGroup,
		}
	}

	firehoseLogDeliveryStream := d.Get("firehose_log_delivery_stream").(*string)
	var firehose *kafkaconnect.FirehoseLogDelivery
	if firehoseLogDeliveryStream != nil {
		firehose = &kafkaconnect.FirehoseLogDelivery{
			DeliveryStream: firehoseLogDeliveryStream,
			Enabled:        &enabled,
		}
	}

	s3LogBucket := d.Get("s3_log_bucket").(*string)
	s3LogPrefix := d.Get("s3_log_prefix").(*string)
	var s3 *kafkaconnect.S3LogDelivery
	if s3LogBucket != nil && s3LogPrefix != nil {
		s3 = &kafkaconnect.S3LogDelivery{
			Bucket:  s3LogBucket,
			Prefix:  s3LogPrefix,
			Enabled: &enabled,
		}
	}

	input := &kafkaconnect.CreateConnectorInput{
		Capacity: &kafkaconnect.Capacity{
			ProvisionedCapacity: &kafkaconnect.ProvisionedCapacity{
				McuCount:    &mcuCount,
				WorkerCount: &workerCount,
			},
		},
		ConnectorConfiguration: connectorConfiguration,
		ConnectorDescription:   &connectorDescription,
		ConnectorName:          &connectorName,
		KafkaCluster: &kafkaconnect.KafkaCluster{
			ApacheKafkaCluster: &kafkaconnect.ApacheKafkaCluster{
				BootstrapServers: &bootstrapServers,
				Vpc: &kafkaconnect.Vpc{
					SecurityGroups: securityGroups,
					Subnets:        subnets,
				},
			},
		},
		KafkaClusterClientAuthentication: &kafkaconnect.KafkaClusterClientAuthentication{
			AuthenticationType: &authType,
		},
		KafkaClusterEncryptionInTransit: &kafkaconnect.KafkaClusterEncryptionInTransit{
			EncryptionType: &encryptionType,
		},
		KafkaConnectVersion: &kafkaConnectVersion,
		LogDelivery: &kafkaconnect.LogDelivery{
			WorkerLogDelivery: &kafkaconnect.WorkerLogDelivery{
				CloudWatchLogs: cloudWatchLogs,
				Firehose:       firehose,
				S3:             s3,
			},
		},
		Plugins:                 plugins,
		ServiceExecutionRoleArn: roleArn,
		WorkerConfiguration:     workerConfiguration,
	}
	return input, nil
}

func loadCustomPlugins(conn *kafkaconnect.KafkaConnect, pluginArns []string) ([]*kafkaconnect.Plugin, error) {
	maxResults := int64(20)
	customPlugins, err := conn.ListCustomPlugins(&kafkaconnect.ListCustomPluginsInput{
		MaxResults: &maxResults,
	})
	if err != nil {
		return nil, err
	}

	var plugins []*kafkaconnect.Plugin
	for _, customPlugin := range customPlugins.CustomPlugins {
		for _, pluginArn := range pluginArns {
			if pluginArn == *customPlugin.CustomPluginArn {
				plugins = append(plugins, &kafkaconnect.Plugin{
					CustomPlugin: &kafkaconnect.CustomPlugin{
						CustomPluginArn: customPlugin.CustomPluginArn,
						Revision:        customPlugin.LatestRevision.Revision,
					},
				})
				continue
			}
		}
	}
	return plugins, nil
}
