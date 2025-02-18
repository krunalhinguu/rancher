package clusterupstreamrefresher

import (
	"context"

	"github.com/kr/pretty"
	akscontroller "github.com/rancher/aks-operator/controller"
	aksv1 "github.com/rancher/aks-operator/pkg/apis/aks.cattle.io/v1"
	mgmtv3 "github.com/rancher/rancher/pkg/generated/norman/management.cattle.io/v3"
	wranglerv1 "github.com/rancher/wrangler/v3/pkg/generated/controllers/core/v1"
	"github.com/sirupsen/logrus"
)

func BuildAKSUpstreamSpec(secretsCache wranglerv1.SecretCache, secretClient wranglerv1.SecretClient, cluster *mgmtv3.Cluster) (*aksv1.AKSClusterConfigSpec, error) {
	ctx := context.Background()
	logrus.Infof("Inside BuildAKSUpstreamSpec for cluster %s", cluster.Name)
	logrus.Infof("BuildAKSUpstreamSpec: cluster.Spec.AKSConfig: %s", pretty.Sprint(cluster.Spec.AKSConfig))

	// Call to build the upstream cluster state
	upstreamSpec, err := akscontroller.BuildUpstreamClusterState(ctx, secretsCache, secretClient, cluster.Spec.AKSConfig)
	if err != nil {
		logrus.Errorf("Failed to build upstream cluster state for AKS cluster [%s]: %v", cluster.Name, err)
		return nil, err
	}
	logrus.Infof("Upstream spec successfully built for AKS cluster %s", pretty.Sprint(upstreamSpec))

	// Set additional values on the upstreamSpec
	upstreamSpec.ClusterName = cluster.Spec.AKSConfig.ClusterName
	upstreamSpec.ResourceLocation = cluster.Spec.AKSConfig.ResourceLocation
	upstreamSpec.ResourceGroup = cluster.Spec.AKSConfig.ResourceGroup
	upstreamSpec.AzureCredentialSecret = cluster.Spec.AKSConfig.AzureCredentialSecret
	upstreamSpec.Imported = cluster.Spec.AKSConfig.Imported

	logrus.Infof("Upstream spec values set for AKS cluster [%s]: ClusterName=%s, ResourceLocation=%s, ResourceGroup=%s, Imported=%v",
		cluster.Name, upstreamSpec.ClusterName, upstreamSpec.ResourceLocation, upstreamSpec.ResourceGroup, upstreamSpec.Imported)

	return upstreamSpec, nil
}
