package aws

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/service/kafkaconnect"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"log"

	"github.com/aws/aws-sdk-go/aws"
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
		},
	}
}

func resourceAwsMskConnectCreate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	conn := meta.(*AWSClient).kafkaconnectconn

	input :=
		&kafkaconnect.CreateConnectorInput{}

	output, err := conn.CreateConnector(input)

	if err != nil {
		return diag.FromErr(fmt.Errorf("error creating MSK Connector: %s", err))
	}

	d.SetId(aws.StringValue(output.ConnectorArn))

	return resourceAwsMskConnectorRead(ctx, d, meta)
}

func resourceAwsMskConnectorRead(_ context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	conn := meta.(*AWSClient).kafkaconnectconn

	configurationInput := &kafkaconnect.DescribeConnectorInput{
		ConnectorArn: aws.String(d.Id()),
	}

	configurationOutput, err := conn.DescribeConnector(configurationInput)

	if isAWSErr(err, kafkaconnect.ErrCodeBadRequestException, "Configuration ARN does not exist") {
		log.Printf("[WARN] MSK Configuration (%s) not found, removing from state", d.Id())
		d.SetId("")
		return nil
	}

	if err != nil {
		return diag.FromErr(fmt.Errorf("error describing MSK Connector (%s): %s", d.Id(), err))
	}

	if configurationOutput == nil {
		return diag.FromErr(fmt.Errorf("error describing MSK Connector (%s): missing result", d.Id()))
	}

	//
	d.Set("arn", configurationOutput.ConnectorArn)

	return diag.Diagnostics{}
}

func resourceAwsMskConnectorUpdate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	conn := meta.(*AWSClient).kafkaconnectconn

	input := &kafkaconnect.UpdateConnectorInput{
		ConnectorArn: aws.String(d.Id()),
	}

	_, err := conn.UpdateConnector(input)

	if err != nil {
		return diag.FromErr(err)
	}

	return resourceAwsMskConnectorRead(ctx, d, meta)
}

func resourceAwsMskConnectorDelete(_ context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	conn := meta.(*AWSClient).kafkaconnectconn

	input := &kafkaconnect.DeleteConnectorInput{
		ConnectorArn: aws.String(d.Id()),
	}

	_, err := conn.DeleteConnector(input)

	if err != nil {
		return diag.FromErr(err)
	}

	return nil
}
