package aws

import (
	"fmt"
	"log"
	"regexp"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/acmpca"
	"github.com/aws/aws-sdk-go/service/kafka"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/acctest"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
	"github.com/terraform-providers/terraform-provider-aws/aws/internal/service/kafka/finder"
	"github.com/terraform-providers/terraform-provider-aws/aws/internal/tfresource"
)

func init() {
	// resource.AddTestSweepers("aws_msk_connector", &resource.Sweeper{
	// 	Name: "aws_msk_connector",
	// 	F:    testSweepMskConnectors,
	// })
}

const (
	MskConnectorPortPlaintext = 9092
	MskConnectorPortSaslScram = 9096
	MskConnectorPortSaslIam   = 9098
	MskConnectorPortTls       = 9094

	MskConnectorPortZookeeper = 2181
)

const (
	MskConnectorBrokerRegexpFormat = `^(([-\w]+\.){1,}[\w]+:%[1]d,){2,}([-\w]+\.){1,}[\w]+:%[1]d+$`
)

var (
	MskConnectorBoostrapBrokersRegexp          = regexp.MustCompile(fmt.Sprintf(MskConnectorBrokerRegexpFormat, MskConnectorPortPlaintext))
	MskConnectorBoostrapBrokersSaslScramRegexp = regexp.MustCompile(fmt.Sprintf(MskConnectorBrokerRegexpFormat, MskConnectorPortSaslScram))
	MskConnectorBoostrapBrokersSaslIamRegexp   = regexp.MustCompile(fmt.Sprintf(MskConnectorBrokerRegexpFormat, MskConnectorPortSaslIam))
	MskConnectorBoostrapBrokersTlsRegexp       = regexp.MustCompile(fmt.Sprintf(MskConnectorBrokerRegexpFormat, MskConnectorPortTls))

	MskConnectorZookeeperConnectStringRegexp = regexp.MustCompile(fmt.Sprintf(MskConnectorBrokerRegexpFormat, MskConnectorPortZookeeper))
)

func TestAccAWSMskConnector_basic(t *testing.T) {
	var cluster kafka.ClusterInfo
	rName := acctest.RandomWithPrefix("tf-acc-test")
	resourceName := "aws_msk_connector.test"

	resource.ParallelTest(t, resource.TestCase{
		PreCheck:   func() { testAccPreCheck(t); testAccPreCheckAWSMsk(t) },
		ErrorCheck: testAccErrorCheck(t, kafka.EndpointsID),
		Providers:  testAccProviders,
		Steps: []resource.TestStep{
			{
				Config: testAccMskConnectorConfig_basic(rName),
				Check: resource.ComposeAggregateTestCheckFunc(
					testAccCheckMskConnectorExists(resourceName, &cluster),
					testAccMatchResourceAttrRegionalARN(resourceName, "arn", "kafka", regexp.MustCompile(`cluster/.+$`)),
					resource.TestCheckResourceAttr(resourceName, "broker_node_group_info.#", "1"),
					resource.TestCheckResourceAttr(resourceName, "broker_node_group_info.0.az_distribution", kafka.BrokerAZDistributionDefault),
					resource.TestCheckResourceAttr(resourceName, "broker_node_group_info.0.ebs_volume_size", "10"),
					resource.TestCheckResourceAttr(resourceName, "broker_node_group_info.0.client_subnets.#", "3"),
					resource.TestCheckTypeSetElemAttrPair(resourceName, "broker_node_group_info.0.client_subnets.*", "aws_subnet.example_subnet_az1", "id"),
					resource.TestCheckTypeSetElemAttrPair(resourceName, "broker_node_group_info.0.client_subnets.*", "aws_subnet.example_subnet_az2", "id"),
					resource.TestCheckTypeSetElemAttrPair(resourceName, "broker_node_group_info.0.client_subnets.*", "aws_subnet.example_subnet_az3", "id"),
					resource.TestCheckResourceAttr(resourceName, "broker_node_group_info.0.instance_type", "kafka.m5.large"),
					resource.TestCheckResourceAttr(resourceName, "broker_node_group_info.0.security_groups.#", "1"),
					resource.TestCheckTypeSetElemAttrPair(resourceName, "broker_node_group_info.0.security_groups.*", "aws_security_group.example_sg", "id"),
					resource.TestCheckResourceAttr(resourceName, "client_authentication.#", "0"),
					resource.TestCheckResourceAttr(resourceName, "cluster_name", rName),
					resource.TestCheckResourceAttr(resourceName, "configuration_info.#", "1"),
					resource.TestCheckResourceAttr(resourceName, "encryption_info.#", "1"),
					testAccMatchResourceAttrRegionalARN(resourceName, "encryption_info.0.encryption_at_rest_kms_key_arn", "kms", regexp.MustCompile(`key/.+`)),
					resource.TestCheckResourceAttr(resourceName, "encryption_info.0.encryption_in_transit.#", "1"),
					resource.TestCheckResourceAttr(resourceName, "encryption_info.0.encryption_in_transit.0.client_broker", "TLS"),
					resource.TestCheckResourceAttr(resourceName, "encryption_info.0.encryption_in_transit.0.in_cluster", "true"),
					resource.TestCheckResourceAttr(resourceName, "enhanced_monitoring", kafka.EnhancedMonitoringDefault),
					resource.TestCheckResourceAttr(resourceName, "kafka_version", "2.7.1"),
					resource.TestCheckResourceAttr(resourceName, "number_of_broker_nodes", "3"),
					resource.TestCheckResourceAttr(resourceName, "tags.%", "0"),
					resource.TestMatchResourceAttr(resourceName, "zookeeper_connect_string", MskConnectorZookeeperConnectStringRegexp),
					resource.TestCheckResourceAttr(resourceName, "bootstrap_brokers", ""),
					resource.TestCheckResourceAttr(resourceName, "bootstrap_brokers_sasl_scram", ""),
					resource.TestMatchResourceAttr(resourceName, "bootstrap_brokers_tls", MskConnectorBoostrapBrokersTlsRegexp),
					testCheckResourceAttrIsSortedCsv(resourceName, "bootstrap_brokers_tls"),
					testCheckResourceAttrIsSortedCsv(resourceName, "zookeeper_connect_string"),
					testCheckResourceAttrIsSortedCsv(resourceName, "zookeeper_connect_string_tls"),
				),
			},
			{
				ResourceName:      resourceName,
				ImportState:       true,
				ImportStateVerify: true,
				ImportStateVerifyIgnore: []string{
					"current_version",
				},
			},
		},
	})
}

func testAccCheckMskConnectorConfigurationExists(resourceName string, configuration *kafka.DescribeConfigurationOutput) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		rs, ok := s.RootModule().Resources[resourceName]
		if !ok {
			return fmt.Errorf("Not found: %s", resourceName)
		}

		if rs.Primary.ID == "" {
			return fmt.Errorf("Resource ID not set: %s", resourceName)
		}

		conn := testAccProvider.Meta().(*AWSClient).kafkaconn

		input := &kafka.DescribeConfigurationInput{
			Arn: aws.String(rs.Primary.ID),
		}

		output, err := conn.DescribeConfiguration(input)

		if err != nil {
			return fmt.Errorf("error describing MSK Cluster (%s): %s", rs.Primary.ID, err)
		}

		*configuration = *output

		return nil
	}
}

func testAccMskClusterConfig_basic(rName string) string {
	return composeConfig(testAccMskClusterBaseConfig(rName), fmt.Sprintf(`
resource "aws_msk_connector" "test" {
  connector_name           = %[1]q
}
`, rName))
}
