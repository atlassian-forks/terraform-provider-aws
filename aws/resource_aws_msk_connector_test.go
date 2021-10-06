package aws

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/service/kafka"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/acctest"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
)

func TestAccAWSMskConnector_basic(t *testing.T) {
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
					resource.TestCheckResourceAttr(resourceName, "connector_name", rName),
					resource.TestCheckResourceAttr(resourceName, "tags.%", "0"),
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

func testAccMskConnectorConfig_basic(rName string) string {
	return composeConfig(testAccMskClusterBaseConfig(rName), fmt.Sprintf(`
resource "aws_msk_connector" "test" {
	connector_name           = %[1]q
	tags = {
		foo = "bar"
	}
}
`, rName))
}
