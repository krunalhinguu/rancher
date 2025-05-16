package ack

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	cs "github.com/alibabacloud-go/cs-20151215/v5/client"
	openapi "github.com/alibabacloud-go/darabonba-openapi/v2/client"
	ecs "github.com/alibabacloud-go/ecs-20140526/v7/client"
	resourcegroup "github.com/alibabacloud-go/resourcemanager-20200331/v3/client"
	util "github.com/alibabacloud-go/tea-utils/v2/service"
	"github.com/alibabacloud-go/tea/tea"
	vpc "github.com/alibabacloud-go/vpc-20160428/v6/client"
	credential "github.com/aliyun/credentials-go/credentials"
)

type Capabilities struct {
	AccessKey string
	SecretKey string
	RegionID  string
}

type regionsResponseBody struct {
	RegionId       string `json:"regionId"`
	LocalName      string `json:"localName"`
	RegionEndpoint string `json:"regionEndpoint,omitempty"`
}

func getCredential(cap *Capabilities) (credential.Credential, error) {
	return credential.NewCredential(&credential.Config{
		Type:            tea.String("access_key"),
		AccessKeyId:     tea.String(cap.AccessKey),
		AccessKeySecret: tea.String(cap.SecretKey),
	})
}

func ListClusters(ctx context.Context, cap *Capabilities) ([]byte, int, error) {
	cred, err := getCredential(cap)
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("failed to create credentials: %w", err)
	}

	config := &openapi.Config{
		Credential: cred,
		RegionId:   tea.String(cap.RegionID),
		Endpoint:   tea.String("cs." + cap.RegionID + ".aliyuncs.com"),
	}

	client, err := cs.NewClient(config)
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("failed to create ACK client: %w", err)
	}

	req := &cs.DescribeClustersV1Request{
		RegionId: tea.String(cap.RegionID),
	}

	runtime := &util.RuntimeOptions{}
	resp, err := client.DescribeClustersV1WithOptions(req, nil, runtime)
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("failed to list cluster: %w", err)
	}

	return encodeOutput(resp.Body.Clusters)
}

func ListRegions(ctx context.Context, cap *Capabilities, acceptLanguage string) ([]byte, int, error) {
	cred, err := getCredential(cap)
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("failed to create credentials: %w", err)
	}

	config := &openapi.Config{
		Credential: cred,
		Endpoint:   tea.String("ecs." + cap.RegionID + ".aliyuncs.com"),
	}

	client, err := ecs.NewClient(config)
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("failed to create ECS client: %w", err)
	}

	request := &ecs.DescribeRegionsRequest{}
	if acceptLanguage != "" {
		request.AcceptLanguage = tea.String(acceptLanguage)
	}

	runtime := &util.RuntimeOptions{}
	response, err := client.DescribeRegionsWithOptions(request, runtime)
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("failed to list regions: %w", err)
	}

	var regions []regionsResponseBody
	for _, r := range response.Body.Regions.Region {
		regions = append(regions, regionsResponseBody{
			RegionId:       tea.StringValue(r.RegionId),
			LocalName:      tea.StringValue(r.LocalName),
			RegionEndpoint: tea.StringValue(r.RegionEndpoint),
		})
	}

	return encodeOutput(regions)
}

func ListZones(ctx context.Context, cap *Capabilities) ([]byte, int, error) {
	cred, err := getCredential(cap)
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("failed to create credentials: %w", err)
	}

	config := &openapi.Config{
		Credential: cred,
		Endpoint:   tea.String("ecs." + cap.RegionID + ".aliyuncs.com"),
	}

	client, err := ecs.NewClient(config)
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("failed to create ECS client: %w", err)
	}

	request := &ecs.DescribeZonesRequest{}
	runtime := &util.RuntimeOptions{}
	response, err := client.DescribeZonesWithOptions(request, runtime)
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("failed to describe zones: %w", err)
	}

	return encodeOutput(response.Body.Zones.Zone)
}

func ListInstanceTypes(ctx context.Context, cap *Capabilities) ([]byte, int, error) {
	cred, err := getCredential(cap)
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("failed to create credentials: %w", err)
	}

	config := &openapi.Config{
		Credential: cred,
		Endpoint:   tea.String("ecs." + cap.RegionID + ".aliyuncs.com"),
	}

	client, err := ecs.NewClient(config)
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("failed to create ECS client: %w", err)
	}

	request := &ecs.DescribeInstanceTypesRequest{}
	runtime := &util.RuntimeOptions{}
	response, err := client.DescribeInstanceTypesWithOptions(request, runtime)
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("failed to describe instance types: %w", err)
	}

	return encodeOutput(response.Body.InstanceTypes.InstanceType)
}

func ListKeyPairs(ctx context.Context, cap *Capabilities) ([]byte, int, error) {
	cred, err := getCredential(cap)
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("failed to create credentials: %w", err)
	}

	config := &openapi.Config{
		Credential: cred,
		Endpoint:   tea.String("ecs." + cap.RegionID + ".aliyuncs.com"),
	}

	client, err := ecs.NewClient(config)
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("failed to create ECS client: %w", err)
	}

	request := &ecs.DescribeKeyPairsRequest{}
	runtime := &util.RuntimeOptions{}
	response, err := client.DescribeKeyPairsWithOptions(request, runtime)
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("failed to describe key pairs: %w", err)
	}

	return encodeOutput(response.Body.KeyPairs.KeyPair)
}

func ListAvailableResources(ctx context.Context, cap *Capabilities, params map[string]string) ([]byte, int, error) {
	cred, err := getCredential(cap)
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("failed to create credentials: %w", err)
	}

	config := &openapi.Config{
		Credential: cred,
		Endpoint:   tea.String("ecs." + cap.RegionID + ".aliyuncs.com"),
	}

	client, err := ecs.NewClient(config)
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("failed to create ECS client: %w", err)
	}

	destResource, ok := params["destinationResource"]
	if !ok || destResource == "" {
		return nil, http.StatusBadRequest, fmt.Errorf("missing required parameter: destinationResource")
	}

	req := &ecs.DescribeAvailableResourceRequest{
		DestinationResource: tea.String(destResource),
	}

	if val, ok := params["instanceChargeType"]; ok {
		req.InstanceChargeType = tea.String(val)
	}
	if val, ok := params["instanceType"]; ok {
		req.InstanceType = tea.String(val)
	}
	if val, ok := params["ioOptimized"]; ok {
		req.IoOptimized = tea.String(val)
	}
	if val, ok := params["networkCategory"]; ok {
		req.NetworkCategory = tea.String(val)
	}
	if val, ok := params["zoneId"]; ok {
		req.ZoneId = tea.String(val)
	}
	if val, ok := params["systemDiskCategory"]; ok {
		req.SystemDiskCategory = tea.String(val)
	}

	runtime := &util.RuntimeOptions{}
	response, err := client.DescribeAvailableResourceWithOptions(req, runtime)
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("failed to describe available resources: %w", err)
	}

	return encodeOutput(response.Body.AvailableZones.AvailableZone)
}

func ListResourceGroups(ctx context.Context, cap *Capabilities, status string) ([]byte, int, error) {
	cred, err := getCredential(cap)
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("failed to create credentials: %w", err)
	}

	client, err := resourcegroup.NewClient(&openapi.Config{
		Credential: cred,
		Endpoint:   tea.String("resourcemanager.aliyuncs.com"),
	})
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("failed to create ResourceManager client: %w", err)
	}

	req := &resourcegroup.ListResourceGroupsRequest{}
	if status != "" {
		req.Status = tea.String(status)
	}

	runtime := &util.RuntimeOptions{}
	resp, err := client.ListResourceGroupsWithOptions(req, runtime)
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("failed to list resource groups: %w", err)
	}

	return encodeOutput(resp.Body.ResourceGroups)
}

func ListVPCs(ctx context.Context, cap *Capabilities, resourceGroupID string) ([]byte, int, error) {
	cred, err := getCredential(cap)
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("failed to create credentials: %w", err)
	}

	config := &openapi.Config{
		Credential: cred,
		Endpoint:   tea.String("vpc." + cap.RegionID + ".aliyuncs.com"),
	}

	client, err := vpc.NewClient(config)
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("failed to create VPC client: %w", err)
	}

	request := &vpc.DescribeVpcsRequest{}
	if resourceGroupID != "" {
		request.ResourceGroupId = tea.String(resourceGroupID)
	}

	runtime := &util.RuntimeOptions{}
	response, err := client.DescribeVpcsWithOptions(request, runtime)
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("failed to describe VPCs: %w", err)
	}

	return encodeOutput(response.Body.Vpcs.Vpc)
}

func ListVSwitches(ctx context.Context, cap *Capabilities, vpcID, zoneID, resourceGroupID string, pageSize, pageNumber int) ([]byte, int, error) {
	cred, err := getCredential(cap)
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("failed to create credentials: %w", err)
	}

	config := &openapi.Config{
		Credential: cred,
		Endpoint:   tea.String("vpc." + cap.RegionID + ".aliyuncs.com"),
	}

	client, err := vpc.NewClient(config)
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("failed to create VPC client: %w", err)
	}

	request := &vpc.DescribeVSwitchesRequest{
		VpcId:           tea.String(vpcID),
		ZoneId:          tea.String(zoneID),
		ResourceGroupId: tea.String(resourceGroupID),
		PageSize:        tea.Int32(int32(pageSize)),
		PageNumber:      tea.Int32(int32(pageNumber)),
	}

	runtime := &util.RuntimeOptions{}
	response, err := client.DescribeVSwitchesWithOptions(request, runtime)
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("failed to describe VSwitches: %w", err)
	}

	return encodeOutput(response.Body.VSwitches.VSwitch)
}

func ListSecurityGroups(ctx context.Context, cap *Capabilities) ([]byte, int, error) {
	cred, err := getCredential(cap)
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("failed to create credentials: %w", err)
	}

	config := &openapi.Config{
		Credential: cred,
		Endpoint:   tea.String("ecs." + cap.RegionID + ".aliyuncs.com"),
	}

	client, err := ecs.NewClient(config)
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("failed to create ECS client: %w", err)
	}

	request := &ecs.DescribeSecurityGroupsRequest{}
	runtime := &util.RuntimeOptions{}
	response, err := client.DescribeSecurityGroupsWithOptions(request, runtime)
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("failed to describe security groups: %w", err)
	}

	return encodeOutput(response.Body.SecurityGroups.SecurityGroup)
}

func ListImages(ctx context.Context, cap *Capabilities, instanceType, imageOwnerAlias, supportIoOptimized string, pageSize, pageNumber int) ([]byte, int, error) {
	cred, err := getCredential(cap)
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("failed to create credentials: %w", err)
	}

	config := &openapi.Config{
		Credential: cred,
		Endpoint:   tea.String("ecs." + cap.RegionID + ".aliyuncs.com"),
	}

	client, err := ecs.NewClient(config)
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("failed to create ECS client: %w", err)
	}

	req := &ecs.DescribeImagesRequest{
		InstanceType:    tea.String(instanceType),
		ImageOwnerAlias: tea.String(imageOwnerAlias),
		PageSize:        tea.Int32(int32(pageSize)),
		PageNumber:      tea.Int32(int32(pageNumber)),
	}

	if supportIoOptimized != "" {
		req.IsSupportIoOptimized = tea.Bool(supportIoOptimized == "true")
	}

	runtime := &util.RuntimeOptions{}
	resp, err := client.DescribeImagesWithOptions(req, runtime)
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("failed to describe images: %w", err)
	}

	return encodeOutput(resp.Body.Images.Image)
}

func encodeOutput(result interface{}) ([]byte, int, error) {
	data, err := json.Marshal(result)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return data, http.StatusOK, nil
}
